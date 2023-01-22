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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Morphic.Telemetry;

internal struct MqttEventMessage
{
    [JsonPropertyName("id")]
    public Guid Id { get; set; }
    //
    [JsonPropertyName("record_type")]
    public string RecordType { get; set; }
    //
    [JsonPropertyName("record_version")]
    public int RecordVersion { get; set; }
    //
    [JsonPropertyName("sent_at")]
    public DateTimeOffset SentAt { get; set; }
    //
    [JsonPropertyName("site_id")]
    public string? SiteId { get; set; }
    //
    [JsonPropertyName("device_id")]
    public string DeviceId { get; set; }
    //
    [JsonPropertyName("software_version")]
    public string SoftwareVersion { get; set; }
    //
    [JsonPropertyName("os_name")]
    public string OsName { get; set; }
    //
    [JsonPropertyName("os_version")]
    public string OsVersion { get; set; }
    //
    [JsonPropertyName("event_name")]
    public string EventName { get; set; }
    //
    [JsonPropertyName("data")]
    public object? Data { get; set; }
}

