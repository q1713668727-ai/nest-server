import { Body, Controller, Post, UploadedFile, UseInterceptors } from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { FileService } from './file.service';

@Controller('file')
export class FileController {
  constructor(private readonly fileService: FileService) {}

  @Post('uploadFile')
  uploadFile(@Body() body: any) {
    return this.fileService.uploadFile(body);
  }

  @Post('uploadEnd')
  uploadEnd(@Body() body: any) {
    return this.fileService.uploadEnd(body);
  }

  @Post('addnote')
  @UseInterceptors(FileInterceptor('file'))
  addnote(@Body() body: any, @UploadedFile() file?: any) {
    return this.fileService.addnote(body, file);
  }

  @Post('addnoteEnd')
  addnoteEnd(@Body() body: any) {
    return this.fileService.addnoteEnd(body);
  }

  @Post('addvideo')
  addvideo(@Body() body: any) {
    return this.fileService.addvideo(body);
  }

  @Post('addvideoEnd')
  addvideoEnd(@Body() body: any) {
    return this.fileService.addvideoEnd(body);
  }
}
