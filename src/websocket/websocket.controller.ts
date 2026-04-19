import { Controller, Get, Query } from '@nestjs/common';
import { WebsocketHttpService } from './websocket-http.service';

@Controller('websocket')
export class WebsocketController {
  constructor(private readonly websocketHttpService: WebsocketHttpService) {}

  @Get('init')
  init(@Query() query: any) {
    return this.websocketHttpService.init(query);
  }

  @Get('getMoreMessage')
  getMoreMessage(@Query() query: any) {
    return this.websocketHttpService.getMoreMessage(query);
  }
}
