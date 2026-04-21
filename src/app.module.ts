import { MiddlewareConsumer, Module, NestModule, RequestMethod } from '@nestjs/common';
import { DbModule } from './db/db.module';
import { AuthModule } from './auth/auth.module';
import { AuthMiddleware } from './auth/auth.middleware';
import { IndexModule } from './index/index.module';
import { LoginModule } from './login/login.module';
import { UserModule } from './user/user.module';
import { FileModule } from './file/file.module';
import { VideoModule } from './video/video.module';
import { WebsocketModule } from './websocket/websocket.module';
import { ConfigModule } from '@nestjs/config';

@Module({
  imports: [
    DbModule,
    AuthModule,
    IndexModule,
    LoginModule,
    UserModule,
    FileModule,
    VideoModule,
    WebsocketModule,
    ConfigModule.forRoot({
      isGlobal: true, // 这样 process.env 才能读取到 .env 文件
    })
  ],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer) {
    consumer
      .apply(AuthMiddleware)
      .exclude(
        { path: 'login', method: RequestMethod.ALL },
        { path: 'login/*path', method: RequestMethod.ALL },
        { path: 'index', method: RequestMethod.POST },
        { path: 'getAllUser', method: RequestMethod.POST },
        { path: 'searchContent', method: RequestMethod.POST },
        { path: 'noteDetail', method: RequestMethod.POST },
        { path: 'video', method: RequestMethod.POST },

        // 兼容无 /public 前缀的旧地址
        { path: 'user-avatar/:account/:file', method: RequestMethod.ALL },
        { path: 'note-image/:account/:file', method: RequestMethod.ALL },
        { path: 'video/:account/:file', method: RequestMethod.ALL },
        { path: 'video-cover/:account/:file', method: RequestMethod.ALL },
        { path: 'user-background/:account/:file', method: RequestMethod.ALL },
        { path: 'user-message/:chat/:file', method: RequestMethod.ALL },

        // 新静态资源前缀 /public
        { path: 'public/user-avatar/:account/:file', method: RequestMethod.ALL },
        { path: 'public/note-image/:account/:file', method: RequestMethod.ALL },
        { path: 'public/video/:account/:file', method: RequestMethod.ALL },
        { path: 'public/video-cover/:account/:file', method: RequestMethod.ALL },
        { path: 'public/user-background/:account/:file', method: RequestMethod.ALL },
        { path: 'public/user-message/:chat/:file', method: RequestMethod.ALL },
      )
      .forRoutes({ path: '*', method: RequestMethod.ALL });
  }
}
