import { Injectable, OnModuleInit } from '@nestjs/common';
import { DbService } from '../db/db.service';

const WebSocket = require('ws');

@Injectable()
export class WebsocketServerService implements OnModuleInit {
  constructor(private readonly db: DbService) {}

  onModuleInit() {
    const wss = new WebSocket.Server({ port: 8002 });
    wss.on('connection', (ws: any) => {
      ws.on('message', async (msg: Buffer) => {
        let objData: any;
        try {
          objData = JSON.parse(msg.toString());
        } catch {
          return;
        }
        if (objData && objData.type === 'register' && objData.account) {
          const currentAccount = String(objData.account);
          wss.clients.forEach((client: any) => {
            if (client !== ws && client.readyState === WebSocket.OPEN && String(client.account || '') === currentAccount) {
              try {
                client.send(JSON.stringify({ type: 'kicked', message: 'Your account logged in elsewhere.' }));
              } catch {}
              try {
                client.close(4001, 'kicked');
              } catch {}
            }
          });
          ws.account = currentAccount;
          try {
            ws.send(JSON.stringify({ type: 201, message: 'registered' }));
          } catch {}
          return;
        }
        if (!objData || !objData.message || !objData.message.text) return;
        if (objData.message.text.type === 'file' && objData.message.text.url) {
          const suffix = objData.message.text.url.split('.');
          objData.message.text.suffix = suffix[suffix.length - 1];
        }
        const targetMessage = { ...objData.message, mine: false };
        const targetUserToUser = `${objData.target}-${objData.account}`;
        const result = await this.db.query<any>('SELECT * FROM `msg` WHERE `UserToUser` = ? OR `UserToUser` = ?;', [objData.UserToUser, targetUserToUser]);
        const chatData: any = { target: null, me: null };
        result.forEach((element) => {
          if (element.UserToUser === targetUserToUser) {
            chatData.target = JSON.parse(element.message);
            chatData.target.read = (chatData.target.read || 0) + 1;
            chatData.target.historyMessage = chatData.target.historyMessage || [];
            chatData.target.historyMessage.push(targetMessage);
          } else if (element.UserToUser === objData.UserToUser) {
            chatData.me = JSON.parse(element.message);
            chatData.me.historyMessage = chatData.me.historyMessage || [];
            chatData.me.historyMessage.push(objData.message);
          }
        });
        if (!chatData.target || !chatData.me) return;
        const first: any = await this.db.query('UPDATE `msg` SET message = ? WHERE `UserToUser` = ?;', [
          JSON.stringify(chatData.target),
          targetUserToUser,
        ]);
        if ((first[0]?.affectedRows ?? first.affectedRows ?? 0) !== 1) return;
        const second: any = await this.db.query('UPDATE `msg` SET message = ? WHERE `UserToUser` = ?;', [
          JSON.stringify(chatData.me),
          objData.UserToUser,
        ]);
        if ((second[0]?.affectedRows ?? second.affectedRows ?? 0) !== 1) return;
        const data = { ...objData };
        if (objData.message.text.type === 'emoji') {
          data.message = { ...data.message, text: { ...data.message.text, url: `images/emoji/${data.message.text.url}` } };
        }
        if (['emoji', 'file', 'text'].includes(objData.message.text.type)) {
          wss.clients.forEach((client: any) => {
            if (
              client.readyState === WebSocket.OPEN &&
              (String(client.account) === String(objData.account) || String(client.account) === String(objData.target))
            ) {
              client.send(JSON.stringify({ type: 200, data }));
            }
          });
        }
      });
    });
  }
}
