// Copyright 2021-2023 Raising the Floor - US, Inc.
//
// Licensed under the New BSD license. You may not use this file except in
// compliance with this License.
//
// You may obtain a copy of the License at
// https://github.com/raisingthefloor/morphic-telemetryclient-lib-cs/blob/main/LICENSE.txt
//
// The R&D leading to these results received funding from the:
// * Rehabilitation Services Administration, US Dept. of Education under
//   grant H421A150006 (APCP)
// * National Institute on Disability, Independent Living, and
//   Rehabilitation Research (NIDILRR)
// * Administration for Independent Living & Dept. of Education under grants
//   H133E080022 (RERC-IT) and H133E130028/90RE5003-01-00 (UIITA-RERC)
// * European Union's Seventh Framework Programme (FP7/2007-2013) grant
//   agreement nos. 289016 (Cloud4all) and 610510 (Prosperity4All)
// * William and Flora Hewlett Foundation
// * Ontario Ministry of Research and Innovation
// * Canadian Foundation for Innovation
// * Adobe Foundation
// * Consumer Electronics Association Foundation

using Morphic.Collections.Persisted;
using Morphic.Core;
using MQTTnet;
using MQTTnet.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Morphic.TelemetryClient;

public class MorphicTelemetryClient : IDisposable
{
    private bool disposedValue;

    // NOTE: TelemetryClientId and its subclasses provide the login credentials and telemetry client id needed when instantiating this class
    public record TelemetryClientConfig
    {
        public readonly string ClientId;
        public readonly string Username;
        public readonly string Password;

        public TelemetryClientConfig(string clientId, string username, string password)
        {
            this.ClientId = clientId;
            this.Username = username;
            this.Password = password;
        }
    }
    //
    public record TcpTelemetryClientConfig : TelemetryClientConfig
    {
        public readonly string Hostname;
        public readonly ushort? Port;
        public readonly bool UseTls;

        public TcpTelemetryClientConfig(string clientId, string username, string password, string hostname, ushort? port = null, bool useTls = false) : base(clientId, username, password)
        {
            this.Hostname = hostname;
            this.Port = port;
            this.UseTls = useTls;
        }
    }
    //
    public record WebsocketTelemetryClientConfig : TelemetryClientConfig
    {
        public readonly string Hostname;
        public readonly ushort? Port;
        public readonly string? Path;
        public readonly bool UseTls;

        public WebsocketTelemetryClientConfig(string clientId, string username, string password, string hostname, ushort? port = null, string? path = null, bool useTls = false) : base(clientId, username, password)
        {
            this.Hostname = hostname;
            this.Port = port;
            this.Path = path;
            this.UseTls = useTls;
        }
    }

    public string? SiteId { get; private set; } = null;

    private IMqttClient _mqttClient;
    private MqttClientOptions _mqttClientOptions;

    private string _clientId;

    private Morphic.Collections.Persisted.AppendOnlyFilePersistedQueue<MqttEventMessage> _persistedQueue;

    private Stopwatch _stopwatchSinceInitialization;
    //
    private Timer _mqttSendEventsTimer;
    private object _mqttSendEventsTimerLock = new();
    //
    private long? _mqttSendEventsTriggerTime;
    private void SetMqttSendEventsTriggerTime(long dueTime, bool onlyIfEarlier = false)
    {
        var triggerTimeSpan = TimeSpan.FromMilliseconds(Math.Max(dueTime, 0));
        var triggerTimeAsMillisecondsSinceInitialization = _stopwatchSinceInitialization.ElapsedMilliseconds + (long)triggerTimeSpan.TotalMilliseconds;

        // if the caller has designated that we only change the trigger time if the new trigger time is earlier--and if an earlier trigger time exists and our new trigger time is not earlier--then abort
        if (onlyIfEarlier == true && ((_mqttSendEventsTriggerTime is not null) && (_mqttSendEventsTriggerTime!.Value < triggerTimeAsMillisecondsSinceInitialization)))
        {
            return;
        }

        lock (_mqttSendEventsTimerLock)
        {
            _mqttSendEventsTriggerTime = triggerTimeAsMillisecondsSinceInitialization;
            _mqttSendEventsTimer.Change(triggerTimeSpan, Timeout.InfiniteTimeSpan);
        }
    }
    private void SetMqttSendEventsTriggerTime(TimeSpan dueTime, bool onlyIfEarlier = false)
    {
        this.SetMqttSendEventsTriggerTime((long)dueTime.TotalMilliseconds, onlyIfEarlier);
    }

    private long? _timestampToDisconnectFromMqttServer = null;
    private void ClearTimestampToDisconnectFromServer()
    {
        _timestampToDisconnectFromMqttServer = null;
    }
    private void SetTimestampToDisconnectFromServer(long dueTimeInMilliseconds)
    {
        _timestampToDisconnectFromMqttServer = _stopwatchSinceInitialization.ElapsedMilliseconds + dueTimeInMilliseconds;
    }
    private void SetTimestampToDisconnectFromServer(TimeSpan dueTime)
    {
        this.SetTimestampToDisconnectFromServer((long)dueTime.TotalMilliseconds);
    }

    // NOTE: these values should ONLY be set by calling MandatoryMinimumBackoffTimestamp's SET accessor; we intentionally did not create a SET accessor for MandatoryMinimumBackoffInterval
    private long? _mandatoryMinimumBackoffInterval = null;
    private long? MandatoryMinimumBackoffInterval
    {
        get
        {
            return _mandatoryMinimumBackoffInterval;
        }
    }
    private long? _mandatoryMinimumBackoffTimestamp = null;
    private long? MandatoryMinimumBackoffTimestamp
    {
        get
        {
            return _mandatoryMinimumBackoffTimestamp;
        }
        set
        {
            if (value is not null)
            {
                var stopwatchElapsedMilliseconds = _stopwatchSinceInitialization.ElapsedMilliseconds;
                var mandatoryMinimumBackoffInterval = Math.Max(0, value!.Value - stopwatchElapsedMilliseconds);
                //
                _mandatoryMinimumBackoffInterval = mandatoryMinimumBackoffInterval;
                _mandatoryMinimumBackoffTimestamp = stopwatchElapsedMilliseconds + mandatoryMinimumBackoffInterval;
            }
            else
            {
                _mandatoryMinimumBackoffInterval = null;
                _mandatoryMinimumBackoffTimestamp = null;
            }
        }
    }

    private bool? _preparingForDisposal = false;
    private bool? _preparedForDisposal = false;
    private AutoResetEvent _preparedForDisposalEvent = new(false);
    private long? _maximumDisposalTimestamp = null;
    private long? MaximumDisposalTimestamp
    {
        get
        {
            return _maximumDisposalTimestamp;
        }
        set
        {
            _maximumDisposalTimestamp = value;

            if (value is not null)
            {
                _preparingForDisposal = true;

                // trigger our state machine to run one more time
                this.SetMqttSendEventsTriggerTime(TimeSpan.Zero);
            }
        }
    }

    // NOTE: this public (helper constructor) sets up a new instance of our TelemetryClient using an empty (and new) persisted queue
    //       [NOTE: as we do not provide the persisted queue with a file path to where it can persist the telemetry records, it will not actually persist the transaction log to disk]
    private MorphicTelemetryClient(TelemetryClientConfig config) : this(config, new() /* new persistedQueue which is not actually persisted */)
    {
    }

    private MorphicTelemetryClient(TelemetryClientConfig config, Morphic.Collections.Persisted.AppendOnlyFilePersistedQueue<MqttEventMessage> persistedQueue)
    {
        // NOTE: in our constructor, we simply intialize variables and then validate and store the provided configuration settings;
        //       it is the job of our state machine (which is triggered by the "enqueued" event from the backing datastore) to actually connect, disconnect, etc.
		// NOTE: we will manually trigger the state machine once near the end of this function is our initial persisted queue is not empty

        // create our MQTT "send events" timer; it will basically handle the state machine of sending telemetry events to the cloud
        _mqttSendEventsTimer = new Timer(this.MqttSendEventsTimerCallback);

        // start a stopwatch which will run for our entire lifetime; we'll use this to track timeout timestamps vs. elapsed MS
        _stopwatchSinceInitialization = Stopwatch.StartNew();

        // initialize and capture our MQTT client and its configuration options
        //
        // create a new MqttClient instance
        var mqttFactory = new MqttFactory();
        _mqttClient = mqttFactory.CreateMqttClient();

        // set up our MqttClient's options
        if (config is TcpTelemetryClientConfig tcpConfig)
        {
            _mqttClientOptions = new MqttClientOptionsBuilder()
                .WithClientId(tcpConfig.ClientId)
                .WithTcpServer(tcpConfig.Hostname, tcpConfig.Port)
                .WithCredentials(tcpConfig.Username, tcpConfig.Password)
                .WithCleanSession(true) // we are a write-only client (i.e. no subscriptions), so always start with a clean session
                .WithTls(new MqttClientOptionsBuilderTlsParameters() { UseTls = tcpConfig.UseTls })
                .Build();

        }
        else if (config is WebsocketTelemetryClientConfig wsConfig)
        {
            var path = wsConfig.Hostname;
            if (wsConfig.Port is not null)
            {
                path += ":" + wsConfig.Port!.Value.ToString();
            }
            else
            {
                path += ":443";
            }
            if (wsConfig.Path is not null)
            {
                // sanity check: make sure the path component starts with "/" or "#"
                if (wsConfig.Path.Length > 0)
                {
                    var firstCharacter = wsConfig.Path.Substring(0, 1);
                    if (firstCharacter != "/" && firstCharacter != "#")
                    {
                        path += "/";
                    }
                }
                //
                path += wsConfig.Path!;
            }

            _mqttClientOptions = new MqttClientOptionsBuilder()
                .WithClientId(wsConfig.ClientId)
                .WithWebSocketServer(path)
                .WithCredentials(wsConfig.Username, wsConfig.Password)
                .WithCleanSession(true) // we are a write-only client (i.e. no subscriptions), so always start with a clean session
                                        //.WithTls(new MqttClientOptionsBuilderTlsParameters() { UseTls = false })
                .WithTls(new MqttClientOptionsBuilderTlsParameters() { UseTls = wsConfig.UseTls })
                .Build();
        }
        else
        {
            throw new ArgumentException("Argument '" + nameof(config) + "' is of an unknown TelemetryClientConfig-related type.");
        }

        // set up connect handler (which will be used by the state machine; this is also a place where we can do any per-connection initialization once we're connected)
        _mqttClient.ConnectedAsync += async (e) =>
        {
            await this.MqttClientConnected(_mqttClient, e);
        };
        // set up disconnect handler (to handle automatic reconnection)
        _mqttClient.DisconnectedAsync += async (e) =>
        {
            await this.MqttClientDisconnected(_mqttClient, e);
        };

        // initialize our client id
        _clientId = config.ClientId;

        // initialize our persisted queue
        _persistedQueue = persistedQueue;
        //
        // if there are already items in our persisted queue, then trigger our events timer callback now (so it can start sending out the events)
        if (_persistedQueue.Count > 0)
        {
            this.SetMqttSendEventsTriggerTime(TimeSpan.Zero);
        }
        //
        // wire up our "item enqueued" event so that we can send out newly-enqueued events as well
        _persistedQueue.ItemEnqueued += _persistedQueue_ItemEnqueued;
    }

    public static MorphicTelemetryClient Create(TelemetryClientConfig config)
    {
        var result = new MorphicTelemetryClient(config);
        return result;
    }

    public record CreateUsingOnDiskTransactionLogError : MorphicAssociatedValueEnum<CreateUsingOnDiskTransactionLogError.Values>
    {
        // enum members
        public enum Values
        {
            AccessDenied,
            FileContentsAreInvalid,
            InvalidPath,
            IOExceptionError/*(IOException exception)*/,
            OtherExceptionError/*(Exception exception)*/
        }

        // functions to create member instances
        public static CreateUsingOnDiskTransactionLogError AccessDenied => new(Values.AccessDenied);
        public static CreateUsingOnDiskTransactionLogError FileContentsAreInvalid => new(Values.FileContentsAreInvalid);
        public static CreateUsingOnDiskTransactionLogError InvalidPath => new(Values.InvalidPath);
        public static CreateUsingOnDiskTransactionLogError IOExceptionError(IOException exception) => new(Values.IOExceptionError) { Exception = exception };
        public static CreateUsingOnDiskTransactionLogError OtherExceptionError(Exception exception) => new(Values.OtherExceptionError) { Exception = exception };

        // associated values
        public Exception? Exception { get; private set; }

        // verbatim required constructor implementation for MorphicAssociatedValueEnums
        private CreateUsingOnDiskTransactionLogError(Values value) : base(value) { }
    }
    //
    public static async Task<MorphicResult<MorphicTelemetryClient, CreateUsingOnDiskTransactionLogError>> CreateUsingOnDiskTransactionLogAsync(TelemetryClientConfig config, string path)
    {
        AppendOnlyFilePersistedQueue<MqttEventMessage> persistedQueue;

        if (System.IO.File.Exists(path) == true)
        {
            // if the file already exists, attempt to open it
            var fromFileResult = await AppendOnlyFilePersistedQueue<MqttEventMessage>.FromFileAsync(path, true /* 'appendNewQueueActionsAfterLoad = true' ensures that our log file continues to be populated with new entries */);
            if (fromFileResult.IsError == true)
            {
                switch (fromFileResult.Error!.Value)
                {
                    case AppendOnlyFilePersistedQueue<MqttEventMessage>.FromFileError.Values.ExceptionError:
                        {
                            var exception = fromFileResult.Error!.Exception!;
                            var typeOfException = exception.GetType();
                            //
                            if (typeOfException == typeof(UnauthorizedAccessException) |
                                typeOfException == typeof(SecurityException))
                            {
                                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.AccessDenied);
                            }
                            else if (typeOfException == typeof(IOException))
                            {
                                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.IOExceptionError((IOException)exception));
                            }
                            else
                            {
                                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.OtherExceptionError(exception));
                            }
                        }
                    case AppendOnlyFilePersistedQueue<MqttEventMessage>.FromFileError.Values.FileContentsAreInvalid:
                        return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.FileContentsAreInvalid);
                    default:
                        throw new MorphicUnhandledErrorException();
                }
            }
            persistedQueue = fromFileResult.Value!;
        }
        else
        {
            try
            {
                // if a file does not already exist at the specified path, attempt to create the file (and then delete it, as we don't leave empty log files on disk)
                // NOTE: we attempt to create and then delete the file because we want the caller to know that there will be access problems with the log later on (when it is eventually created)
                var createdFileStream = System.IO.File.Create(path);
                await createdFileStream.DisposeAsync();
                System.IO.File.Delete(path);

                // once we know we can write to a file path, create a persisted queue using that path
                persistedQueue = new AppendOnlyFilePersistedQueue<MqttEventMessage>(path);
            }
            catch (UnauthorizedAccessException)
            {
                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.AccessDenied);
            }
            catch (ArgumentNullException)
            {
                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.InvalidPath);
            }
            catch (ArgumentException)
            {
                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.InvalidPath);
            }
            catch (PathTooLongException)
            {
                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.InvalidPath);
            }
            catch (DirectoryNotFoundException)
            {
                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.InvalidPath);
            }
            catch (NotSupportedException)
            {
                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.InvalidPath);
            }
            catch (IOException ex)
            {
                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.IOExceptionError(ex));
            }
            catch (Exception ex)
            {
                return MorphicResult.ErrorResult(CreateUsingOnDiskTransactionLogError.OtherExceptionError(ex));
            }
        }

        // create a telemetry client using the loaded/created persisted queue
        var telemetryClient = new MorphicTelemetryClient(config, persistedQueue);

        return MorphicResult.OkResult(telemetryClient);
    }

    // NOTE: this function tells the TelemetryClient to finish sending any currently-outstanding event message and to shut down its server connection (in preparation for disposal);
    //       it should also instruct the backing queue to flush the current transaction log out to disk (so that later disposal is basically instantaneous)
    public async Task PrepareForDisposalAsync(TimeSpan? maximumTimeSpan = null)
    {
        if (_preparedForDisposal is null)
        {
            throw new Exception("This function may only be called once.");
        }

        if (maximumTimeSpan is not null)
        {
            // set our class instance's MaximumDisposalTimestamp value; it will use this to know how long it can try to send data before needing to shut down
            // NOTE: setting this property will set _preparingForDisposal to true and trigger our state machine to execute immediatley
            this.MaximumDisposalTimestamp = _stopwatchSinceInitialization.ElapsedMilliseconds + (long)maximumTimeSpan!.Value.TotalMilliseconds;
        }

        // wait for our state machine to complete
        var cancellationTokenSource = new CancellationTokenSource();
        var cancellationToken = cancellationTokenSource.Token;
        var task = Task.Run(() =>
        {
            if (maximumTimeSpan is not null)
            {
                _preparedForDisposalEvent.WaitOne(maximumTimeSpan!.Value);
            } 
            else
            {
                _preparedForDisposalEvent.WaitOne();
            }
            cancellationTokenSource.Cancel();
        });
        //
        if (maximumTimeSpan is not null)
        {
            try
            {
                var remainingTimeSpan = (int)Math.Max(0, this.MaximumDisposalTimestamp!.Value - ((long)maximumTimeSpan!.Value.TotalMilliseconds / 2) - _stopwatchSinceInitialization.ElapsedMilliseconds);
                Task.Delay(remainingTimeSpan, cancellationToken).GetAwaiter().GetResult();
            }
            catch (TaskCanceledException)
            {
                // task was ended before timeout, which is good behavior; continue
            }
        }
        else
        {
            await task;
        }

        // flush our persisted queue to disk
		// NOTE: the current persisted queue's FlushToDiskAsync function might not return quickly when the network connection is unavailable, so we run it as a 
		//       parallel task; in the future, we may want to modify the corresponding persisted queue code to ensure that it always returns within a specified timespan
        cancellationTokenSource = new CancellationTokenSource();
        cancellationToken = cancellationTokenSource.Token;
        task = Task.Run(async () => {
            await _persistedQueue.FlushToDiskAsync();
            cancellationTokenSource.Cancel();
        });
        if (maximumTimeSpan is not null)
        {
            try
            {
                var remainingTimeSpan = (int)Math.Max(0, this.MaximumDisposalTimestamp!.Value - _stopwatchSinceInitialization.ElapsedMilliseconds);
                Task.Delay(remainingTimeSpan, cancellationToken).GetAwaiter().GetResult();
            }
            catch (TaskCanceledException)
            {
                // task was ended before timeout, which is good behavior; continue
            }
        }
        else
        {
            await task;
        }
    }

    // NOTE: in this function, we dispose of our backing queue (which will also make sure that its transaction log file is closed); note that technically this is not
    //       necessary, as the append-only backing queue's file should always be in a good state--but it will help ensure that all pending data is flushed out to disk.
    protected virtual void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            if (disposing)
            {
                // dispose managed state (managed objects)
                // flush our persisted queue to disk
                var cancellationTokenSource = new CancellationTokenSource();
                var cancellationToken = cancellationTokenSource.Token;
                var task = Task.Run(() => {
                    _persistedQueue.FlushToDiskAsync().GetAwaiter().GetResult();
                    cancellationTokenSource.Cancel();
                });
                try
                {
                    // wait a maximum of 250ms for the persisted queue to flush to disk
                    var maximumWait = TimeSpan.FromMilliseconds(250);
                    Task.Delay(maximumWait, cancellationToken).GetAwaiter().GetResult();
                }
                catch (TaskCanceledException)
                {
                    // task was ended before timeout, which is good behavior; continue
                }
            }

            // TODO: free unmanaged resources (unmanaged objects) and override finalizer
            // TODO: set large fields to null
            disposedValue = true;
        }
    }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    //

    public void SetSiteId(string? value)
    {
        // NOTE: while it's fine for the user to set the site id multiple times, we might need to take additional actions regarding changes in this value in the future;
        //       out of an abundance of caution, we're using the SetXXX pattern for this variable (instead of simply treating it as a field).
        // NOTE: the site ID should _only_ apply to newly-enqueued events
        this.SiteId = value;
    }

    //

    // NOTE: the object passed in as "data" must be JSON-serializable
    public void EnqueueEvent(string eventName, object? data)
    {
        // NOTE: we capture the timestamp up front just to alleviate any potential for the timestamp to be delay-captured
        var capturedAtTimestamp = DateTimeOffset.UtcNow;

        var actionMessage = new MqttEventMessage()
        {
            Id = Guid.NewGuid(),
            RecordType = "event",
            RecordVersion = 1,
            SentAt = capturedAtTimestamp,
            SiteId = this.SiteId,
            DeviceId = _clientId,
            SoftwareVersion = this.CachedSoftwareVersion.Value,
            OsName = "Windows",
            OsVersion = this.CachedOsVersion.Value,
            EventName = eventName,
            Data = data
        };

        // enqueue the action message; we'll receive an ItemEnqueued event in response to this (which we'll then use to trigger our timer to write out any queued message)
        _persistedQueue.Enqueue(actionMessage);
    }

    private void _persistedQueue_ItemEnqueued(object? sender, Collections.Persisted.AppendOnlyFilePersistedQueue<MqttEventMessage>.ItemEnqueuedEventArgs e)
    {
        // fire our "MQTT Send Events" timer so that it can process our newly-enqueued event
		// NOTE: the MQTT Send Events timer's state machine will process all enqueued events, including ones enqueued before this one, in order.
        this.SetMqttSendEventsTriggerTime(TimeSpan.Zero);
    }

    //

    // NOTE: we capture the OS version once during run (since it should not change at runtime)
    private Lazy<string> CachedOsVersion = new Lazy<string>(() =>
    {
        return System.Environment.OSVersion.Version.ToString();
    });

    // NOTE: we capture the software version once during run (since it should not change at runtime)
    private Lazy<string> CachedSoftwareVersion = new Lazy<string>(() =>
    {
        return Assembly.GetEntryAssembly()?.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion ?? "0.0.0.0";
    });

    //

    #region State Machine

    // NOTE: this function is the heart of our state machine.  When it is called, it checks the state of our persisted queue as well as the state of our connection, our various timeouts and intervals, etc.; it is also responsible for re-triggering itself in scenarios where it needs to retry again later
    private readonly SemaphoreSlim _mqttSendEventsTimerCallbackReentrySemaphore = new SemaphoreSlim(1, 1);
    private async void MqttSendEventsTimerCallback(object? state)
    {
        /* NOTES on how our state machine (coordinated via this function) works, the scenarios it needs to handle, etc.:
         * 
         * If there are no enqueued events, the only thing we might need to do is disconnect our connection (if it should be timed out); there is no need to restart a timer in this scenario
         * ...
         * OTHERWISE, if there are enqueued events...
        // 1. if there are events and we are not connected, attempt to connect now (unless we failed recently and have a mandatory backoff period; if that's the case then retrigger ourself to run at the end of that backoff period)
        // 2. if there are events and we are already connected, send those messages now
        // 3. if we cannot connect, set a timer (and a timestamp to check) to retry again a little later; also maybe persist out our queue immediately (or just let it persist itself after its regular period of time)
        // 4. if we can connect or are connected and we can't send events, then terminate the connection and set an event (and timestamp) to retry after a backoff period (or find another reasonable response to this scenario)
        // 5. if we are connected and have sent all events, then set a timer/timestamp indicating when when we should disconnect.
        // 6. if there are events waiting to be written, ABSOLUTELY IGNORE the "disconnect after" timestamp!
        // 7. if we fail to connect, we should clear the "disconnect after" timestamp/timer (and when we connect, we should re-set the disconnect-after timestamp/timer)
         */

        if (_preparedForDisposal == true)
        {
            // if our class is prepared for disposal, we should not do any further work
            return;
        }

        const int MQTT_SERVER_CONNECTION_STAYALIVE_TIME_IN_SECONDS = 60;

        TimeSpan MINIMUM_BACKOFF_INTERVAL = TimeSpan.FromSeconds(60);
        TimeSpan MAXIMUM_BACKOFF_INTERVAL = TimeSpan.FromMinutes(30);

        // NOTE: we can set a retry trigger if our function is already executing
        TimeSpan? retryTriggerTimeSpanDueToReentryProtection = null;

        // NOTE: we can set a trigger for the time at which we should disconnect from the MQTT server (to make sure we disconnect appropriately if no more data is ready to be sent)
        TimeSpan? disconnectFromMqttServerAfterTimespan = null;

        // NOTE: we _must_ protect this function against re-entry (just in case the timer fires while we're still working, due to the user changing the persistance time interval etc.)
        var semaphoreAvailable = await _mqttSendEventsTimerCallbackReentrySemaphore.WaitAsync(0);
        if (semaphoreAvailable == true) 
        {
            // execute our state machine logic
            try
            {
                // step 1: determine if there are events to be enqueued
                bool itemsAreEnqueued = _persistedQueue.Count > 0;

                if (itemsAreEnqueued == true)
                {
                    // if items are enqueued, then dequeue them and move them to the MQTT broker in the cloud

                    // first, connect if we're not already connected
                    if (_mqttClient.IsConnected == false)
                    {
                        var mandatoryMinimumBackoffTimestamp = this.MandatoryMinimumBackoffTimestamp;
                        if (mandatoryMinimumBackoffTimestamp is not null && mandatoryMinimumBackoffTimestamp > _stopwatchSinceInitialization.ElapsedMilliseconds)
                        {
                            // if we have a mandatory backoff time before reconnecting, and if that time has not elapsed, then don't do anything; we'll re-trigger this function at the backoff time in the final section of this function
                        }
                        else
                        {
                            // otherwise, attempt to connect
                            // NOTE: if we'd like to set a timeout on connection, we can pass in a cancellation token here (and then we could wait only a certain timespan before cancelling that token)
                            MqttClientConnectResult? connectResult;
                            try
                            {
                                connectResult = await _mqttClient.ConnectAsync(_mqttClientOptions, CancellationToken.None);
                            }
                            catch
                            {
                                connectResult = null;
                            }
                            if (connectResult is not null && connectResult!.ResultCode == MqttClientConnectResultCode.Success)
                            {
                                // we are connected
                                // first, clear out any mandatory minimum backoff times (since we connected successfully)
                                this.MandatoryMinimumBackoffTimestamp = null;

                                // and clear out any timestamps related to disconnecting from the server
                                this.ClearTimestampToDisconnectFromServer();
                            }
                            else
                            {
                                // if we failed to connect, establish aa minimum backoff interval (or double the previous interval, up to a maximum point) and then we'll try again later
                                if (this.MandatoryMinimumBackoffInterval is null)
                                {
                                    this.MandatoryMinimumBackoffTimestamp = _stopwatchSinceInitialization.ElapsedMilliseconds + (long)MINIMUM_BACKOFF_INTERVAL.TotalMilliseconds;
                                }
                                else
                                {
                                    // double the previous interval
                                    var newBackoffInterval = this.MandatoryMinimumBackoffInterval ?? MINIMUM_BACKOFF_INTERVAL.TotalMilliseconds;
                                    newBackoffInterval *= 2;
                                    if (newBackoffInterval > MAXIMUM_BACKOFF_INTERVAL.TotalMilliseconds)
                                    {
                                        newBackoffInterval = MAXIMUM_BACKOFF_INTERVAL.TotalMilliseconds;
                                    }
                                    this.MandatoryMinimumBackoffTimestamp = _stopwatchSinceInitialization.ElapsedMilliseconds + (long)newBackoffInterval;
                                }
                            }
                        }
                    }

                    // if we are now connected, try sending events!
                    if (_mqttClient.IsConnected == true)
                    {
                        while (_preparingForDisposal == false)
                        {
                            var peekResult = _persistedQueue.Peek();
                            if (peekResult.IsError == true)
                            {
                                // nothing to dequeue
                                break;
                            }
                            var messageToSend = peekResult.Value!;

                            // convert the message to JSON
                            var payload = JsonSerializer.Serialize(messageToSend);

                            // assemble the mqtt message
                            var mqttMessage = new MqttApplicationMessageBuilder()
                                .WithTopic("telemetry")
                                .WithPayload(payload)
                                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce) // NOTE: what's the default if we don't specify this?
                                .WithRetainFlag() // NOTE: what's the default if we don't specify this?
                                .Build();

                            // send the mqtt message
                            bool publishSuccess;
                            try
                            {
                                var publishResult = await _mqttClient.PublishAsync(mqttMessage, CancellationToken.None);
                                if (publishResult.ReasonCode == MqttClientPublishReasonCode.Success)
                                {
                                    publishSuccess = true;
                                }
                                else
                                {
                                    publishSuccess = false;
                                }
                            }
                            catch
                            {
                                publishSuccess = false;
                            }

                            if (publishSuccess == false)
                            {
                                // if publishing the message was not successful, disconnect and then break out of this loop
                                // NOTE: to prevent agressive connections, message failures, disconnections and then reconnections, set a mandatory backoff period
                                this.MandatoryMinimumBackoffTimestamp = _stopwatchSinceInitialization.ElapsedMilliseconds + (long)MINIMUM_BACKOFF_INTERVAL.TotalMilliseconds;

                                await _mqttClient.DisconnectAsync();
                                break;
                            }
                            else
                            {
                                // dequeue the message (so that it's not sent again)
                                var dequeueResult = _persistedQueue.Dequeue();
                                if (dequeueResult.IsError == true)
                                {
                                    // if we could not dequeue, then we have a significant error (as no other code should be dequeueing)
                                    throw new Exception("Failed to dequeue item which was peeked, found and ready for dequeue.");
                                }
                            }
                        }

                        if (_preparedForDisposal == false)
                        {
                            // as there were no more events to enqueue, set a disconnect timer
                            disconnectFromMqttServerAfterTimespan = TimeSpan.FromSeconds((double)MQTT_SERVER_CONNECTION_STAYALIVE_TIME_IN_SECONDS);

                            this.SetTimestampToDisconnectFromServer((long)disconnectFromMqttServerAfterTimespan!.Value.TotalMilliseconds);
                        }
                    }
                }
                else // if (itemsAreEnqueued == false)
                {
                    // if items are NOT enqueued, then we might need to set up a disconnect time at which we should disconnect our MQTT client (i.e. a timeout period after the last connection or after the last message sent to the server)
                    if (_mqttClient.IsConnected == true && _timestampToDisconnectFromMqttServer is null)
                    {
                        // the MQTT client is connected, but there are no entries enqueued so we should disconnect after the prescribed amount of time
                        disconnectFromMqttServerAfterTimespan = TimeSpan.FromSeconds((double)MQTT_SERVER_CONNECTION_STAYALIVE_TIME_IN_SECONDS);

                        this.SetTimestampToDisconnectFromServer((long)disconnectFromMqttServerAfterTimespan!.Value.TotalMilliseconds);
                    }
                    else if (_mqttClient.IsConnected == true && _timestampToDisconnectFromMqttServer is not null)
                    {
                        var stopwatchSinceInitializationElapsedMilliseconds = _stopwatchSinceInitialization.ElapsedMilliseconds;
                        if (_timestampToDisconnectFromMqttServer!.Value <= stopwatchSinceInitializationElapsedMilliseconds)
                        {
                            // if our disconnect time has come, then disconnect now
                            this.ClearTimestampToDisconnectFromServer();
                            // disconnect from our MQTT server in a normal fashion
                            // NOTE: if we need to send a last will or alter our disconnection strategy, do so here.
                            await _mqttClient.DisconnectAsync(MqttClientDisconnectReason.NormalDisconnection);

                            disconnectFromMqttServerAfterTimespan = null;
                        }
                        else
                        {
                            // if our disconnect time has not elapsed, then set the corresponding timespan trigger value now
                            disconnectFromMqttServerAfterTimespan = TimeSpan.FromMilliseconds((double)(_timestampToDisconnectFromMqttServer!.Value - stopwatchSinceInitializationElapsedMilliseconds));
                        }
                    }

                    // we have finished any/all work we might have to do here (since there is nothing to do other than disconnect if there are no items enqueued)
                }

                // if our telemetry client is preparing for disposal, go ahead and indicate that we have completed our current work (i.e. are prepared for disposal)
                if (_preparingForDisposal == true)
                {
                    _preparedForDisposal = true;
                    _preparedForDisposalEvent.Set();

                    // if we are prepared for disposal, there is nothing more to do 
                    return;
                }
            }
            finally
            {
                // free our reentry lock
                _mqttSendEventsTimerCallbackReentrySemaphore.Release();
            }
        } 
        else
        {
            // if our timer is already executing, then reconfigure our timer to fire in five seconds; this will effectively wait no more than five seconds to re-fire after the previous call completes (even if multiple changes get fired in rapid succession)
            retryTriggerTimeSpanDueToReentryProtection = new TimeSpan(0, 0, 5);
        }


        // timespans to pay attention to when determining which timespan to use to re-trigger this state machine
        // - retryTriggerTimeSpanDueToReentryProtection (non-null if we should re-trigger this function because it was already executing)
        // - disconnectFromMqttServerAfterTimespan (non-null if we should disconnect from the MQTT server after a certain timespan)
        // - this.MandatoryMinimumBackoffInterval (non-null if we should wait at least a certain amount of time before reconnecting)
        // - _maximumDisposalTimestamp (non-null if there's a maximum time at which we should terminate our actions), indirectly via the _prepareForDisposal and _preparedForDisposal flags

        TimeSpan? runStateMachineAgainAfterTimespan = null;

        // if we need to re-run due to our function already running, set our trigger time to match
        if (retryTriggerTimeSpanDueToReentryProtection is not null)
        {
            runStateMachineAgainAfterTimespan = retryTriggerTimeSpanDueToReentryProtection!.Value;
        }

        // if we should disconnect our connection after a certain timespan, use that timespan for our run-again trigger instead (if it's sooner)
        if (disconnectFromMqttServerAfterTimespan is not null)
        {
            if (runStateMachineAgainAfterTimespan is null || disconnectFromMqttServerAfterTimespan!.Value < runStateMachineAgainAfterTimespan!.Value)
            {
                runStateMachineAgainAfterTimespan = disconnectFromMqttServerAfterTimespan!.Value;
            }
        }

        // if we have a mandatory timestamp we must wait before trying to reconnect, make sure we call the state machine again when that time is triggered
        long? mandatoryMinimumBackoffInterval = this.MandatoryMinimumBackoffInterval;
        if (mandatoryMinimumBackoffInterval is not null)
        {
            TimeSpan mandatoryMinimumBackoffTimespan = TimeSpan.FromMilliseconds((double)mandatoryMinimumBackoffInterval);
            if (runStateMachineAgainAfterTimespan is null || mandatoryMinimumBackoffTimespan < runStateMachineAgainAfterTimespan!.Value)
            {
                runStateMachineAgainAfterTimespan = mandatoryMinimumBackoffTimespan;
            }
        }

        // if we should prepare for disposal but we are not yet fully prepared for disposal, re-trigger this function immediately
        if (_preparingForDisposal == true && _preparedForDisposal == false)
        {
            runStateMachineAgainAfterTimespan = TimeSpan.Zero;
        }

        if (runStateMachineAgainAfterTimespan is not null)
        {
            this.SetMqttSendEventsTriggerTime(runStateMachineAgainAfterTimespan!.Value, /*onlyIfEarlier: */true);
        }
    }

    private async Task MqttClientConnected(IMqttClient mqttClient, MQTTnet.Client.MqttClientConnectedEventArgs e)
    {
        // we might choose to log the connection here, but in our current implementation there's no requirement to do anything once connected (as our state machine is on a separate thread and it handles waiting for the connection to complete)
    }

    private async Task MqttClientDisconnected(IMqttClient sender, MQTTnet.Client.MqttClientDisconnectedEventArgs e)
    {
        // if we've been disconnected, that's usually fine (and usually something we did)
        // but if we've been disconnected (due to the server rebooting, for instance), then re-trigger our state machine so it can reconnect

        if (_preparingForDisposal == true)
        {
            // if we're preparing for (or we've been prepared for) disposal, then do not try to reconnect
            return;
        }

        bool itemsAreEnqueued = _persistedQueue.Count > 0;
        if (itemsAreEnqueued == false)
        {
            // nothing to do; this is a normal situation (i.e. we disconnected and there's nothing to send) so exit
            return;
        }

        // if we reach here, the disconnection was not expected and we should re-connect
        if (this.MandatoryMinimumBackoffTimestamp is not null)
        {
            // if there's a mandatory minimum backoff timestamp, don't try to reconnect until its time (and we will double it, up to a certain point, if we fail to reconnect)
            var backoffMillisecondsFromNow = Math.Max(0, this.MandatoryMinimumBackoffTimestamp!.Value - (long)_stopwatchSinceInitialization.ElapsedMilliseconds);
            this.SetMqttSendEventsTriggerTime(backoffMillisecondsFromNow, /*onlyIfEarlier: */true);
        }
        else
        {
            // if there's no mandatory minimum backoff timestamp, try to reconnect immediately; if the connection fails, we'll set a minimum backoff interval in our state machine (right after the connection fails)
            this.SetMqttSendEventsTriggerTime(TimeSpan.Zero);
        }
    }

#endregion State Machine
}