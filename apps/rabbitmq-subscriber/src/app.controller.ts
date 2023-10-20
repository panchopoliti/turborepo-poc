import { Controller } from '@nestjs/common';
import { AppService } from './app.service';
import {
  Ctx,
  MessagePattern,
  Payload,
  RmqContext,
} from '@nestjs/microservices';
import { MongoClient } from 'mongodb';
import * as sharedUtils from 'shared-utils';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {
    sharedUtils.subtract(1, 2);
  }

  @MessagePattern({ cmd: 'create' })
  async handleCreateEvent(
    @Payload('payload') payload: any,
    @Ctx() context: RmqContext,
  ): Promise<void> {
    console.log('Received create event');

    const channel = context.getChannelRef();
    const originalMsg = context.getMessage();
    const message = JSON.parse(originalMsg.content.toString());

    console.log('message', message);

    sharedUtils.subtract(1, 2);

    channel.ack(originalMsg);
    console.log('message acknowledged');

    await this.toDb({ movieName: message.payload.title });

    // Process the event as needed

    // Acknowledge the message (if required)
    // Note: This depends on your specific use case and whether you need manual acknowledgement
    // If using manual acknowledgements, you might need to adjust your RabbitMQ setup
  }

  private async toDb({ movieName }: { movieName: string }) {
    const uri = process.env.MONGO_URL as string;
    const client = new MongoClient(uri);

    try {
      const database = client.db('sample_mflix');
      const movies = database.collection('movies');
      await movies.insertOne({ title: movieName });
    } finally {
      // Ensures that the client will close when you finish/error
      await client.close();
    }
  }
}
