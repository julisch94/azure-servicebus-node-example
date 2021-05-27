import {
  delay,
  ProcessErrorArgs,
  ServiceBusClient,
  ServiceBusReceivedMessage,
} from "@azure/service-bus";

const connectionString = "<YOUR CONNECTION STRING>";
const queueName = "<YOUR QUEUE NAME>";
const messages = [
  { body: "Albert Einstein" },
  { body: "Werner Heisenberg" },
  { body: "Marie Curie" },
  { body: "Steven Hawking" },
  { body: "Isaac Newton" },
  { body: "Niels Bohr" },
  { body: "Michael Faraday" },
  { body: "Galileo Galilei" },
  { body: "Johannes Kepler" },
  { body: "Nikolaus Kopernikus" },
];

async function main(): Promise<void> {
  await send();

  await receive();
}

async function send(): Promise<void> {
  console.log("Sending...");

  const sbClient = new ServiceBusClient(connectionString);

  const sender = sbClient.createSender(queueName);

  try {
    // Tries to send all messages in a single batch.
    // Will fail if the messages cannot fit in a batch.
    // await sender.sendMessages(messages);

    // create a batch object
    let batch = await sender.createMessageBatch();
    for (let i = 0; i < messages.length; i++) {
      // for each message in the array

      // try to add the message to the batch
      if (!batch.tryAddMessage(messages[i])) {
        // if it fails to add the message to the current batch
        // send the current batch as it is full
        await sender.sendMessages(batch);

        // then, create a new batch
        batch = await sender.createMessageBatch();

        // now, add the message failed to be added to the previous batch to this batch
        if (!batch.tryAddMessage(messages[i])) {
          // if it still can't be added to the batch, the message is probably too big to fit in a batch
          throw new Error("Message too big to fit in a batch");
        }
      }
    }

    // Send the last created batch of messages to the queue
    await sender.sendMessages(batch);

    console.log(`Sent a batch of messages to the queue: ${queueName}`);

    // Close the sender
    await sender.close();
  } finally {
    await sbClient.close();
  }
}

async function receive(): Promise<void> {
  console.log("Receiving...");

  const sbClient = new ServiceBusClient(connectionString);

  const receiver = sbClient.createReceiver(queueName);

  const myMessageHandler = async (
    messageReceived: ServiceBusReceivedMessage
  ) => {
    console.log(`Received message: ${messageReceived.body}`);
  };

  const myErrorHandler = async (error: ProcessErrorArgs) => {
    console.log(error);
  };

  receiver.subscribe({
    processMessage: myMessageHandler,
    processError: myErrorHandler,
  });

  await delay(20000);

  await receiver.close();
  await sbClient.close();
}

main().catch((err) => {
  console.log("Error occurred: ", err);
  process.exit(1);
});
