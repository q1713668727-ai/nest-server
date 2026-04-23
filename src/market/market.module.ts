import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { AuthModule } from '../auth/auth.module';
import { MarketController } from './market.controller';
import { MarketOrderQueueProcessor } from './market-order-queue.processor';
import { MarketService } from './market.service';

@Module({
  imports: [
    AuthModule,
    BullModule.registerQueue({
      name: 'market-order',
    }),
  ],
  controllers: [MarketController],
  providers: [MarketService, MarketOrderQueueProcessor],
})
export class MarketModule {}
