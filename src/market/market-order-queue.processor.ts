import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { MARKET_ORDER_QUEUE, MarketService } from './market.service';

@Processor(MARKET_ORDER_QUEUE)
export class MarketOrderQueueProcessor extends WorkerHost {
  constructor(private readonly marketService: MarketService) {
    super();
  }

  async process(job: Job<{ orderId: number }>) {
    if (job.name !== 'cancel-unpaid-order') return;
    const orderId = Number(job.data?.orderId || 0);
    if (!orderId) return;
    await this.marketService.cancelUnpaidOrder(orderId);
  }
}
