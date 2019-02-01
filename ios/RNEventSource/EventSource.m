//
//  EventSource.m
//  EventSource
//
//  Created by Neil on 25/07/2013.
//  Copyright (c) 2013 Neil Cowburn. All rights reserved.
//

#import "EventSource.h"
#import <CoreGraphics/CGBase.h>

static CGFloat const ES_RETRY_INTERVAL = 1.0;
static CGFloat const ES_DEFAULT_TIMEOUT = 300.0;
static int const ES_LINEBUFFER_LIMIT = 32768;

static NSString *const ESKeyValueDelimiter = @":";
static NSString *const ESEventSeparatorLFLF = @"\n\n";
static NSString *const ESEventSeparatorCRCR = @"\r\r";
static NSString *const ESEventSeparatorCRLFCRLF = @"\r\n\r\n";
static NSString *const ESEventKeyValuePairSeparator = @"\n";

static NSString *const ESEventDataKey = @"data";
static NSString *const ESEventIDKey = @"id";
static NSString *const ESEventAuthKey = @"auth";
static NSString *const ESEventEventKey = @"event";
static NSString *const ESEventRetryKey = @"retry";

@interface EventSource () <NSURLSessionDataDelegate> {
    BOOL wasClosed;
    dispatch_queue_t messageQueue;
    dispatch_queue_t connectionQueue;
}

@property (nonatomic, strong) NSURL *eventURL;
@property (atomic, strong) NSURLSession *session;
@property (nonatomic, strong) NSMutableDictionary *listeners;
@property (nonatomic, assign) NSTimeInterval timeoutInterval;
@property (nonatomic, assign) NSTimeInterval retryInterval;
@property (nonatomic, strong) id lastEventID;
/// Options, including request headers.
@property (nonatomic, strong) NSDictionary *options;
@property (atomic, assign) long httpStatus;
@property (atomic, strong) NSMutableData *httpResponseData;

- (void)_open;
- (void)_dispatchEvent:(Event *)e;

@end

@implementation EventSource {
    Event *_bufferedEvent;
    NSString *_lineBuffer;
}

+ (instancetype)eventSourceWithURL:(NSURL *)URL options:(NSDictionary *)options
{
    return [[EventSource alloc] initWithURL:URL options:options];
}
//timeoutInterval:ES_DEFAULT_TIMEOUT]
// timeoutInterval:(NSTimeInterval)timeoutInterval
// ES_RETRY_INTERVAL

- (instancetype)initWithURL:(NSURL *)URL options:(NSDictionary *)options
{
    self = [super init];
    if (self) {
        _listeners = [NSMutableDictionary dictionary];
        _eventURL = URL;
        _timeoutInterval = ES_DEFAULT_TIMEOUT;
        _retryInterval = ES_RETRY_INTERVAL;
        [self resetBufferedEvent];
        _lineBuffer = nil;
        
        self.options = options;
        
        if (self.options[@"timeout_interval"] != nil) {
            _timeoutInterval = [((NSNumber*)self.options[@"timeout_interval"]) doubleValue];
        }
        
        if (self.options[@"retry_interval"] != nil) {
            _timeoutInterval = [((NSNumber*)self.options[@"retry_interval"]) doubleValue];
        }

        messageQueue = dispatch_queue_create("co.cwbrn.eventsource-queue", DISPATCH_QUEUE_SERIAL);
        connectionQueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_HIGH, 0);

        wasClosed = NO;
        dispatch_time_t popTime = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(_retryInterval * NSEC_PER_SEC));
        dispatch_after(popTime, connectionQueue, ^(void){
            [self _open];
        });
    }
    return self;
}

- (void)resetBufferedEvent
{
    _bufferedEvent = [Event new];
    _bufferedEvent.readyState = kEventStateOpen;
}

- (void)addEventListener:(NSString *)eventName handler:(EventSourceEventHandler)handler
{
    if (self.listeners[eventName] == nil) {
        [self.listeners setObject:[NSMutableArray array] forKey:eventName];
    }
    
    [self.listeners[eventName] addObject:handler];
}

- (void)onMessage:(EventSourceEventHandler)handler
{
    [self addEventListener:MessageEvent handler:handler];
}

- (void)onError:(EventSourceEventHandler)handler
{
    [self addEventListener:ErrorEvent handler:handler];
}

- (void)onOpen:(EventSourceEventHandler)handler
{
    [self addEventListener:OpenEvent handler:handler];
}

- (void)onReadyStateChanged:(EventSourceEventHandler)handler
{
    [self addEventListener:ReadyStateEvent handler:handler];
}

- (void)close
{
    wasClosed = YES;
    [self.session invalidateAndCancel];
}

// -----------------------------------------------------------------------------------------------------------------------------------------

- (void)URLSession:(NSURLSession *)session dataTask:(NSURLSessionDataTask *)dataTask
didReceiveResponse:(NSURLResponse *)response completionHandler:(void (^)(NSURLSessionResponseDisposition disposition))completionHandler
{
    NSHTTPURLResponse *httpResponse = (NSHTTPURLResponse *)response;
    self.httpStatus = httpResponse.statusCode;
    if (httpResponse.statusCode == 200) {
        // Opened
        Event *e = [Event new];
        e.readyState = kEventStateOpen;

        [self _dispatchEvent:e type:ReadyStateEvent];
        [self _dispatchEvent:e type:OpenEvent];
    } else {
        self.httpResponseData = [[NSMutableData alloc] init];
    }

    if (completionHandler) {
        completionHandler(NSURLSessionResponseAllow);
    }
}

- (void)URLSession:(NSURLSession *)session dataTask:(NSURLSessionDataTask *)dataTask didReceiveData:(NSData *)data
{
    @synchronized (self) {
        NSString *eventString = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
        if(_lineBuffer != nil) {
            if((_lineBuffer.length + eventString.length) < ES_LINEBUFFER_LIMIT) {
                eventString = [_lineBuffer stringByAppendingString:eventString];
            }
            else {
                NSLog(@"EventSource line buffer truncated to prevent overflow");
                _lineBuffer = nil;
                eventString = @"";
            }
        }
        NSArray *lines = [eventString componentsSeparatedByCharactersInSet:[NSCharacterSet newlineCharacterSet]];
        NSString* lastLine = [lines lastObject];
        // If lastline is not empty, stream did not end in a new line and parsing should be deffered.
        if(lastLine != nil && lastLine.length > 0) {
            lines = [lines subarrayWithRange:(NSMakeRange(0, [lines count] - 1))];
            _lineBuffer = lastLine;
        } else {
            _lineBuffer = nil;
        }

        if (self.httpResponseData != nil) {
            [self.httpResponseData appendData: data];
        }
        
        for (NSString *line in lines) {
            if ([line hasPrefix:ESKeyValueDelimiter]) {
                continue;
            }

            if (!line || line.length == 0) {
                if(_bufferedEvent.data != nil) {
                    Event* dispatch = _bufferedEvent;
                    dispatch_async(messageQueue, ^{
                        [self _dispatchEvent:dispatch];
                    });
                }
                [self resetBufferedEvent];
                continue;
            }

            @autoreleasepool {
                NSScanner *scanner = [NSScanner scannerWithString:line];
                scanner.charactersToBeSkipped = [NSCharacterSet whitespaceCharacterSet];

                NSString *key, *value;
                [scanner scanUpToString:ESKeyValueDelimiter intoString:&key];
                [scanner scanString:ESKeyValueDelimiter intoString:nil];
                [scanner scanUpToCharactersFromSet:[NSCharacterSet newlineCharacterSet] intoString:&value];

                if (key && value) {
                    if ([key isEqualToString:ESEventEventKey]) {
                        _bufferedEvent.event = value;
                    } else if ([key isEqualToString:ESEventDataKey]) {
                        if (_bufferedEvent.data != nil) {
                            _bufferedEvent.data = [_bufferedEvent.data stringByAppendingFormat:@"\n%@", value];
                        } else {
                            _bufferedEvent.data = value;
                        }
                    } else if ([key isEqualToString:ESEventIDKey]) {
                        _bufferedEvent.id = value;
                        self.lastEventID = _bufferedEvent.id;
                    } else if ([key isEqualToString:ESEventRetryKey]) {
                        self.retryInterval = [value doubleValue];
                    } else {
                        NSLog(@"Received invalid event key: %@", key);
                    }
                } else {
                    NSLog(@"Received invalid event stream line: '%@'", line);
                }
            }
        }
    }
}

- (void)URLSession:(NSURLSession *)session task:(NSURLSessionTask *)task didCompleteWithError:(nullable NSError *)error
{
    [self.session finishTasksAndInvalidate];
    self.session = nil;

    if (wasClosed) {
        return;
    }

    Event *e = [Event new];
    NSString *bodyString = nil;
    if (self.httpResponseData != nil) {
        bodyString = [[NSString alloc] initWithData:self.httpResponseData encoding:NSUTF8StringEncoding];
        self.httpResponseData = nil;
    }
    e.readyState = kEventStateClosed;
    e.error = [NSError errorWithDomain:@""
                                  code:e.readyState
                              userInfo:@{ NSLocalizedDescriptionKey: (error ? [error localizedDescription] : @"Connection with the event source was closed."),
                                          @"status": [NSNumber numberWithLong: (self.httpStatus ?: 0)],
                                          @"body": (bodyString ?: (id)kCFNull)}];

    [self _dispatchEvent:e type:ReadyStateEvent];
    [self _dispatchEvent:e type:ErrorEvent];

    dispatch_time_t popTime = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(_retryInterval * NSEC_PER_SEC));
    dispatch_after(popTime, connectionQueue, ^(void){
        [self _open];
    });
}

// -------------------------------------------------------------------------------------------------------------------------------------

- (void)_open
{
    if (wasClosed)
        return;
    
    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:self.eventURL
                                                           cachePolicy:NSURLRequestReloadIgnoringCacheData
                                                       timeoutInterval:self.timeoutInterval];
    if (self.lastEventID) {
        [request setValue:self.lastEventID forHTTPHeaderField:@"Last-Event-ID"];
    }
    
    NSDictionary *headers = (NSDictionary*)self.options[@"headers"];
    if (headers != nil) {
        for (NSString *header in headers) {
            NSString *value = headers[header];
            [request setValue:value forHTTPHeaderField:header];
        }
    }
    
    self.session = [NSURLSession sessionWithConfiguration:[NSURLSessionConfiguration defaultSessionConfiguration]
                                                 delegate:self
                                            delegateQueue:[NSOperationQueue currentQueue]];
    
    self.httpStatus = 0;
    self.httpResponseData = nil;

    NSURLSessionDataTask *eventSourceTask = [self.session dataTaskWithRequest:request];
    [eventSourceTask resume];

    Event *e = [Event new];
    e.readyState = kEventStateConnecting;

    [self _dispatchEvent:e type:ReadyStateEvent];

    if (![NSThread isMainThread]) {
        CFRunLoopRun();
    }
}

- (void)_dispatchEvent:(Event *)event type:(NSString * const)type
{
    NSArray *errorHandlers = self.listeners[type];
    for (EventSourceEventHandler handler in errorHandlers) {
        dispatch_async(connectionQueue, ^{
            handler(event);
        });
    }
}

- (void)_dispatchEvent:(Event *)event
{
    [self _dispatchEvent:event type:MessageEvent];

    if (event.event != nil) {
        [self _dispatchEvent:event type:event.event];
    }
}

@end

// ---------------------------------------------------------------------------------------------------------------------

@implementation Event

- (NSString *)description
{
    NSString *state = nil;
    switch (self.readyState) {
        case kEventStateConnecting:
            state = @"CONNECTING";
            break;
        case kEventStateOpen:
            state = @"OPEN";
            break;
        case kEventStateClosed:
            state = @"CLOSED";
            break;
    }
    
    return [NSString stringWithFormat:@"<%@: readyState: %@, id: %@; event: %@; data: %@>",
            [self class],
            state,
            self.id,
            self.event,
            self.data];
}

@end

NSString *const MessageEvent = @"message";
NSString *const ErrorEvent = @"error";
NSString *const OpenEvent = @"open";
NSString *const ReadyStateEvent = @"readyState";

