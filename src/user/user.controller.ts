import { Body, Controller, Post, Req } from '@nestjs/common';
import { UserService } from './user.service';

@Controller('user')
export class UserController {
  constructor(private readonly userService: UserService) {}

  @Post('getUserInfo')
  getUserInfo(@Body() body: any) {
    return this.userService.getUserInfo(body);
  }

  @Post('followStatus')
  followStatus(@Req() req: any, @Body() body: any) {
    return this.userService.followStatus(req, body);
  }

  @Post('toggleFollow')
  toggleFollow(@Req() req: any, @Body() body: any) {
    return this.userService.toggleFollow(req, body);
  }

  @Post('followList')
  followList(@Req() req: any, @Body() body: any) {
    return this.userService.followList(req, body);
  }

  @Post('addLikeNote')
  addLikeNote(@Body() body: any) {
    return this.userService.addLikeNote(body);
  }

  @Post('addCollectNote')
  addCollectNote(@Body() body: any) {
    return this.userService.addCollectNote(body);
  }

  @Post('myNote')
  myNote(@Body() body: any) {
    return this.userService.myNote(body);
  }

  @Post('findLikeNote')
  findLikeNote(@Body() body: any) {
    return this.userService.findLikeNote(body);
  }

  @Post('findCollectNote')
  findCollectNote(@Body() body: any) {
    return this.userService.findCollectNote(body);
  }

  @Post('receivedInteractions')
  receivedInteractions(@Req() req: any, @Body() body: any) {
    return this.userService.receivedInteractions(req, body);
  }

  @Post('setBackground')
  setBackground(@Body() body: any) {
    return this.userService.setBackground(body);
  }

  @Post('setBackgroundEnd')
  setBackgroundEnd(@Body() body: any) {
    return this.userService.setBackgroundEnd(body);
  }

  @Post('marketAddresses')
  marketAddresses(@Req() req: any) {
    return this.userService.marketAddresses(req);
  }

  @Post('saveMarketAddress')
  saveMarketAddress(@Req() req: any, @Body() body: any) {
    return this.userService.saveMarketAddress(req, body);
  }

  @Post('deleteMarketAddress')
  deleteMarketAddress(@Req() req: any, @Body() body: any) {
    return this.userService.deleteMarketAddress(req, body);
  }

  @Post('setDefaultMarketAddress')
  setDefaultMarketAddress(@Req() req: any, @Body() body: any) {
    return this.userService.setDefaultMarketAddress(req, body);
  }
}
