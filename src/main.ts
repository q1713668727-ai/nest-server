import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { json, urlencoded } from 'express';
import { createReadStream, statSync } from 'fs';
import { join } from 'path';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule);
  app.enableCors();
  app.use(json({ limit: '50mb' }));
  app.use(urlencoded({ limit: '50mb', extended: true }));

  app.use('/video/:account/:file', (req, res, next) => {
    try {
      const account = String(req.params.account || '').replace(/[^a-zA-Z0-9_-]/g, '');
      const file = String(req.params.file || '');
      if (!account || !/^[^/\\]+$/.test(file)) {
        next();
        return;
      }

      const filePath = join(process.cwd(), 'public', 'video', account, file);
      const stat = statSync(filePath);
      const fileSize = stat.size;
      const range = req.headers.range;
      const contentType = file.toLowerCase().endsWith('.mov') ? 'video/quicktime' : 'video/mp4';

      res.setHeader('Accept-Ranges', 'bytes');
      res.setHeader('Content-Type', contentType);

      if (!range) {
        res.setHeader('Content-Length', fileSize);
        createReadStream(filePath).pipe(res);
        return;
      }

      const match = /^bytes=(\d*)-(\d*)$/.exec(range);
      if (!match) {
        res.status(416).setHeader('Content-Range', `bytes */${fileSize}`).end();
        return;
      }

      const startText = match[1];
      const endText = match[2];
      let start = startText ? Number(startText) : 0;
      let end = endText ? Number(endText) : fileSize - 1;

      if (!startText && endText) {
        const suffixLength = Number(endText);
        start = Math.max(fileSize - suffixLength, 0);
        end = fileSize - 1;
      }

      if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || end < start || start >= fileSize) {
        res.status(416).setHeader('Content-Range', `bytes */${fileSize}`).end();
        return;
      }

      end = Math.min(end, fileSize - 1);
      res.status(206);
      res.setHeader('Content-Range', `bytes ${start}-${end}/${fileSize}`);
      res.setHeader('Content-Length', end - start + 1);
      createReadStream(filePath, { start, end }).pipe(res);
    } catch {
      next();
    }
  });

  // 兼容历史地址：/note-image/... /video/... 等
  app.useStaticAssets(join(process.cwd(), 'public'));

  // 同时支持 /public/note-image/... /public/video/...
  app.useStaticAssets(join(process.cwd(), 'public'), {
    prefix: '/public/',
  });

  app.use((req, res, next) => {
    res.header('Cross-Origin-Opener-Policy', 'same-origin');
    res.setTimeout(120 * 1000, () => {
      res.status(408).send('Request timeout');
    });
    next();
  });
  await app.listen(process.env.PORT ?? 8000);
}
bootstrap();
