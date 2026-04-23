import { Injectable } from '@nestjs/common';
import { DbService } from '../db/db.service';

@Injectable()
export class WebsocketHttpService {
  constructor(private readonly db: DbService) {}

  private async msg(data: any[]) {
    if (!data.length) return [];
    const arr: any[] = [];
    const targetAccounts = new Set<string>();
    for (let i = 0; i < data.length; i += 1) {
      let parsedMessage: any = {};
      try {
        parsedMessage = JSON.parse(data[i].message || '{}');
      } catch {
        parsedMessage = {};
      }
      arr[i] = { ...parsedMessage };
      const targetId = String(arr[i].id || '').trim();
      if (targetId) targetAccounts.add(targetId);
      const array: any[] = [];
      const historyList = Array.isArray(arr[i].historyMessage) ? arr[i].historyMessage : [];
      let idx = 0;
      for (let j = historyList.length - 1; j < historyList.length; j -= 1) {
        if (idx === 20 || j < 0) break;
        idx += 1;
        const messageItem = historyList[j];
        if (messageItem?.text?.type === 'emoji') {
          messageItem.text.url = `images/emoji/${messageItem.text.url}`;
        } else if (messageItem?.text?.type === 'file' && messageItem?.text?.url) {
          const suffix = String(messageItem.text.url).split('.');
          messageItem.text.suffix = suffix[suffix.length - 1];
        }
        array.unshift(messageItem);
      }
      arr[i].historyMessage = array;
    }

    if (targetAccounts.size) {
      const accounts = Array.from(targetAccounts);
      const placeholders = accounts.map(() => '?').join(',');
      const profileRows = await this.db.query<any>(
        `SELECT \`account\`,\`name\`,\`avatar\` FROM \`login\` WHERE \`account\` IN (${placeholders});`,
        accounts,
      );
      const profileMap = new Map(profileRows.map((item: any) => [String(item.account || '').trim(), item]));
      arr.forEach((item) => {
        const id = String(item.id || '').trim();
        const profile = profileMap.get(id);
        const latestAvatar = String(profile?.avatar || item.avatar || '').trim();
        if (profile?.name) item.title = String(profile.name);
        item.url = latestAvatar;
        item.avatar = undefined;
      });
    } else {
      arr.forEach((item) => {
        item.url = String(item.avatar || '').trim();
        item.avatar = undefined;
      });
    }

    return arr;
  }

  private addMsg(data: any[], length: number, target: string) {
    let obj = {};
    if (data.length) {
      for (let i = 0; i < data.length; i += 1) {
        if (JSON.parse(data[i].message).id !== target) continue;
        data[i].message = JSON.parse(data[i].message);
        obj = {
          id: data[i].message.id,
          title: data[i].message.title,
          historyMessage: data[i].message.historyMessage,
        };
        const start = Math.max((obj as any).historyMessage.length - (length + 20), 0);
        const end = (obj as any).historyMessage.length - (length - 1);
        (obj as any).historyMessage = (obj as any).historyMessage.slice(start, end);
        for (let j = 0; j < (obj as any).historyMessage.length; j += 1) {
          if ((obj as any).historyMessage[j].text.type === 'emoji') {
            (obj as any).historyMessage[j].text.url = `images/emoji/${(obj as any).historyMessage[j].text.url}`;
          } else if ((obj as any).historyMessage[j].text.type === 'file') {
            const suffix = (obj as any).historyMessage[j].text.url.split('.');
            (obj as any).historyMessage[j].text.suffix = suffix[1];
          }
        }
        break;
      }
      return { status: 200, result: obj };
    }
    return { status: 404, result: obj };
  }

  async init(query: any) {
    const account = query.account;
    const results = await this.db.query<any>('SELECT * FROM `login` LEFT JOIN `msg` ON msg.account = login.account WHERE msg.account = ?;', [account]);
    return {
      status: 200,
      result: await this.msg(results),
    };
  }

  async getMoreMessage(query: any) {
    const account = query.account;
    const length = parseInt(query.length, 10);
    const target = query.target;
    const results = await this.db.query<any>('SELECT * FROM `login` LEFT JOIN `msg` ON msg.account = login.account WHERE msg.account = ?;', [account]);
    return this.addMsg(results, length, target);
  }
}
