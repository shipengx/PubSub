package com.shipeng;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class SubscriberTest {

    // use the default project id
    private static final String PROJECT_ID = "adara-pbm-qa";
    private static final BlockingQueue<PubsubMessage> messages = new LinkedBlockingDeque<>();
    private static ExecutorService executorService = Executors.newFixedThreadPool(1);

    static class MessageReceiverExample implements MessageReceiver {
        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            messages.offer(message);
            consumer.ack();
        }
    }

    /** Receive messages over a subscription. */
    public static void main(String... args) throws Exception {

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {

                    String jsonCredentialFile = "/opt/opinmind/conf/credentials/adara-pbm-qa-1b3f1f3c826c.json";
                    FixedCredentialsProvider credentialsProvider = FixedCredentialsProvider.create(ServiceAccountCredentials
                            .fromStream(new FileInputStream(jsonCredentialFile))
                            .createScoped(Arrays.asList("https://www.googleapis.com/auth/pubsub")));

                    // set subscriber id, eg. my-sub
                    String subscriptionId = "my-sub";
                    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(
                            PROJECT_ID, subscriptionId);
                    Subscriber subscriber = null;
                    try {
                        // create a subscriber bound to the asynchronous message receiver
                        subscriber =
                                Subscriber.newBuilder(subscriptionName, new MessageReceiverExample())
                                        .setCredentialsProvider(credentialsProvider).build();
                        subscriber.startAsync().awaitRunning();
                        // Continue to listen to messages
                        while (true) {
                            PubsubMessage message = messages.take();
                            System.out.println("Message Id: " + message.getMessageId());
                            System.out.println("Data: " + message.getData().toStringUtf8());
                        }
                    } finally {
                        if (subscriber != null) {
                            subscriber.stopAsync();
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });

        System.out.println("done");

    }// end main

}
