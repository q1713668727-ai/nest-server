import { Body, Controller, Get, Param, Post, Query, Req } from '@nestjs/common';
import { MarketService } from './market.service';

@Controller('market')
export class MarketController {
  constructor(private readonly marketService: MarketService) {}

  @Get('home')
  home() {
    return this.marketService.home();
  }

  @Get('products')
  products(@Query() query: any) {
    return this.marketService.products(query);
  }

  @Get('categories/:id/children')
  categoryChildren(@Param('id') id: string) {
    return this.marketService.categoryChildren(id);
  }

  @Get('products/:id')
  product(@Param('id') id: string) {
    return this.marketService.product(id);
  }

  @Get('products/:id/reviews')
  productReviews(@Param('id') id: string) {
    return this.marketService.productReviews(id);
  }

  @Get('coupons')
  coupons(@Query() query: any) {
    return this.marketService.coupons(query);
  }

  @Get('my-coupons')
  myCoupons(@Req() req: any) {
    return this.marketService.myCoupons(req);
  }

  @Get('orders')
  orders(@Req() req: any) {
    return this.marketService.orders(req);
  }

  @Post('orders/create')
  createOrder(@Req() req: any, @Body() body: any) {
    return this.marketService.createOrder(req, body);
  }

  @Post('orders/confirm-receipt')
  confirmReceipt(@Req() req: any, @Body() body: any) {
    return this.marketService.confirmReceipt(req, body);
  }

  @Post('orders/cancel')
  cancelOrder(@Req() req: any, @Body() body: any) {
    return this.marketService.cancelOrder(req, body);
  }

  @Post('orders/address')
  updateOrderAddress(@Req() req: any, @Body() body: any) {
    return this.marketService.updateOrderAddress(req, body);
  }

  @Post('orders/refund')
  applyOrderRefund(@Req() req: any, @Body() body: any) {
    return this.marketService.applyOrderRefund(req, body);
  }

  @Post('orders/refund/cancel')
  cancelOrderRefund(@Req() req: any, @Body() body: any) {
    return this.marketService.cancelOrderRefund(req, body);
  }

  @Post('orders/review')
  reviewOrder(@Req() req: any, @Body() body: any) {
    return this.marketService.reviewOrder(req, body);
  }

  @Post('receive-coupon')
  receiveCoupon(@Req() req: any, @Body() body: any) {
    return this.marketService.receiveCoupon(req, body);
  }

  @Get('service/session')
  serviceSession(@Req() req: any, @Query() query: any) {
    return this.marketService.serviceSession(req, query);
  }

  @Get('service/sessions')
  serviceSessions(@Req() req: any) {
    return this.marketService.serviceSessions(req);
  }

  @Post('service/message')
  sendServiceMessage(@Req() req: any, @Body() body: any) {
    return this.marketService.sendServiceMessage(req, body);
  }

  @Post('service/session/delete')
  deleteServiceSession(@Req() req: any, @Body() body: any) {
    return this.marketService.deleteServiceSession(req, body);
  }

  @Post('orders/refund/review')
  reviewOrderRefund(@Req() req: any, @Body() body: any) {
    return this.marketService.reviewOrderRefund(req, body);
  }

  @Get('shops/:id')
  shop(@Param('id') id: string, @Query() query: any) {
    return this.marketService.shop(id, query);
  }
}
