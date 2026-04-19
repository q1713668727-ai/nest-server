import { Module } from '@nestjs/common';
import { WebsocketController } from './websocket.controller';
import { WebsocketHttpService } from './websocket-http.service';
import { WebsocketServerService } from './websocket-server.service';

@Module({
  controllers: [WebsocketController],
  providers: [WebsocketHttpService, WebsocketServerService],
})
export class WebsocketModule {}
