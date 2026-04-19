import { Body, Controller, Post, UploadedFile, UseInterceptors } from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { VideoService } from './video.service';

@Controller('video')
export class VideoController {
  constructor(private readonly videoService: VideoService) {}

  @Post()
  list(@Body() body: any) {
    return this.videoService.list(body);
  }

  @Post('addvideo')
  @UseInterceptors(FileInterceptor('chunk'))
  addvideo(@Body() body: any, @UploadedFile() file?: any) {
    return this.videoService.addvideo(body, file);
  }

  @Post('addvideoEnd')
  addvideoEnd(@Body() body: any) {
    return this.videoService.addvideoEnd(body);
  }
}
