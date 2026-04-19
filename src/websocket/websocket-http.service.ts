import { Injectable } from '@nestjs/common';
import { DbService } from '../db/db.service';

@Injectable()
export class WebsocketHttpService {
  constructor(private readonly db: DbService) {}

  private msg(data: any[]) {
    if (!data.length) return [];
    const arr: any[] = [];
    for (let i = 0; i < data.length; i += 1) {
      data[i].message = JSON.parse(data[i].message);
      arr[i] = { ...data[i].message };
      arr[i].url = arr[i].avatar;
      arr[i].avatar = undefined;
      const array: any[] = [];
      let idx = 0;
      for (let j = arr[i].historyMessage.length - 1; j < arr[i].historyMessage.length; j -= 1) {
        if (idx === 20 || j < 0) break;
        idx += 1;
        if (arr[i].historyMessage[j].text.type === 'emoji') {
          arr[i].historyMessage[j].text.url = `images/emoji/${arr[i].historyMessage[j].text.url}`;
        } else if (arr[i].historyMessage[j].text.type === 'file') {
          const suffix = arr[i].historyMessage[j].text.url.split('.');
          arr[i].historyMessage[j].text.suffix = suffix[1];
        }
        array.unshift(arr[i].historyMessage[j]);
      }
      arr[i].historyMessage = array;
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
      result: this.msg(results),
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
