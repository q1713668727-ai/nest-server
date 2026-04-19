import { Body, Controller, Post } from '@nestjs/common';
import { IndexService } from './index.service';

@Controller()
export class IndexController {
  constructor(private readonly indexService: IndexService) {}

  @Post('index')
  index(@Body() body: any) {
    return this.indexService.index(body);
  }

  @Post('noteDetail')
  noteDetail(@Body() body: any) {
    return this.indexService.noteDetail(body);
  }

  @Post('clearBadge')
  clearBadge(@Body() body: any) {
    return this.indexService.clearBadge(body);
  }

  @Post('deleteUser')
  deleteUser(@Body() body: any) {
    return this.indexService.deleteUser(body);
  }

  @Post('setUserData')
  setUserData(@Body() body: any) {
    return this.indexService.setUserData(body);
  }

  @Post('getAllUser')
  getAllUser(@Body() body: any) {
    return this.indexService.getAllUser(body);
  }

  @Post('getConversation')
  getConversation(@Body() body: any) {
    return this.indexService.getConversation(body);
  }

  @Post('add')
  add(@Body() body: any) {
    return this.indexService.add(body);
  }

  @Post('upload/addComment')
  addComment(@Body() body: any) {
    return this.indexService.addComment(body);
  }
}
