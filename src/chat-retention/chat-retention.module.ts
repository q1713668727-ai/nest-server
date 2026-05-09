import { Global, Module } from '@nestjs/common';
import { ChatRetentionService } from './chat-retention.service';

@Global()
@Module({
  providers: [ChatRetentionService],
  exports: [ChatRetentionService],
})
export class ChatRetentionModule {}
