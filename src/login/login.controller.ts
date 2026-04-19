import { Body, Controller, Post, Req } from '@nestjs/common';
import { LoginService } from './login.service';

@Controller('login')
export class LoginController {
  constructor(private readonly loginService: LoginService) {}

  @Post()
  login(@Req() req: any, @Body() body: any) {
    return this.loginService.login(req, body);
  }

  @Post('logout')
  logout(@Req() req: any) {
    return this.loginService.logout(req);
  }

  @Post('reg')
  reg(@Body() body: any) {
    return this.loginService.reg(body);
  }

  @Post('regAvatar')
  regAvatar(@Body() body: any) {
    return this.loginService.regAvatar(body);
  }

  @Post('regAvatarEnd')
  regAvatarEnd(@Body() body: any) {
    return this.loginService.regAvatarEnd(body);
  }
}
