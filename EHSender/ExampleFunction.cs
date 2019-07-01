using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EventHub_Trigger_Example
{
    public static class ExampleFunction
    {
        [FunctionName("ExampleEventHubFunction")]
        public static async Task Run(
            [EventHubTrigger("", Connection = "InputConnectionString")] EventData[] events,
            [EventHub("", Connection = "EH1Output")] IAsyncCollector<EventData> eh1,
            [EventHub("", Connection = "EH2Output")] IAsyncCollector<EventData> eh2,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                    log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");

                    var packageType = eventData.Properties["packagetype"].ToString();

                    switch (packageType)
                    {
                        case "p1":
                        {
                            var eventDataOut = new EventData(Encoding.UTF8.GetBytes(messageBody + " on p1 EH1"));
                            eventDataOut.Properties.Add("correlation-id", eventData.SystemProperties["correlation-id"].ToString());
                            await eh1.AddAsync(eventDataOut);
                            log.LogInformation("Sent to event hub 1");
                            break;
                        }

                        case "p2":
                        {
                            var eventDataOut = new EventData(Encoding.UTF8.GetBytes(messageBody + " on p2 EH2"));
                            eventDataOut.Properties.Add("correlation-id", eventData.SystemProperties["correlation-id"].ToString());
                            await eh2.AddAsync(eventDataOut);
                            log.LogInformation("Sent to event hub 2");
                            break;
                        }
                    }
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
