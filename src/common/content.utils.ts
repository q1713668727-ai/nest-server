export const getSafeAccountPath = (account: string) => String(account || '').replace(/[^a-zA-Z0-9_-]/g, '');

export const timestampToTime = (timestamp: number) => {
  const date = new Date(Number(timestamp));
  const zero = (n: number) => String(n).padStart(2, '0');
  const y = `${date.getFullYear()}-`;
  const m = `${zero(date.getMonth() + 1)}-`;
  const d = `${zero(date.getDate())} `;
  const h = `${zero(date.getHours())}:`;
  const mi = `${zero(date.getMinutes())}:`;
  const s = `${zero(date.getSeconds())}`;
  return y + m + d + h + mi + s;
};

export const parseAccountList = (text: string) =>
  String(text || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);

export const listToText = (list: string[]) =>
  Array.from(new Set((list || []).map((item) => String(item).trim()).filter(Boolean))).join(',');

export const toSafeNumber = (value: any, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

export const normalizeLikeUsers = (value: any) => {
  if (Array.isArray(value)) return value;
  if (!value) return [];
  if (typeof value === 'string') {
    const text = value.trim();
    if (!text) return [];
    try {
      const parsed = JSON.parse(text);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return text
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean)
        .map((account) => ({ account }));
    }
  }
  return [];
};

export const parseNoteComments = (raw: any) => {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  const text = String(raw).trim();
  if (!text) return [];
  try {
    const parsed = JSON.parse(text);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    const items = text.split(';/').filter(Boolean);
    return items
      .map((item) => {
        try {
          return JSON.parse(item);
        } catch {
          return null;
        }
      })
      .filter(Boolean);
  }
};

export const parseContentRefs = (text: string, defaultType = 'note') => {
  const tokens = String(text || '')
    .split(',')
    .map((item) => String(item || '').trim())
    .filter(Boolean);
  const seen = new Set<string>();
  const refs: { contentType: 'note' | 'video'; id: number; key: string }[] = [];
  tokens.forEach((token) => {
    let contentType: 'note' | 'video' = defaultType === 'video' ? 'video' : 'note';
    let rawId = token;
    if (token.startsWith('note-')) {
      contentType = 'note';
      rawId = token.slice(5);
    } else if (token.startsWith('video-')) {
      contentType = 'video';
      rawId = token.slice(6);
    }
    const id = Number(rawId);
    if (!Number.isFinite(id)) return;
    const key = `${contentType}-${id}`;
    if (seen.has(key)) return;
    seen.add(key);
    refs.push({ contentType, id, key });
  });
  return refs;
};

export const normalizeNoteItem = (item: any) => ({
  ...item,
  contentType: 'note',
  feedKey: `note-${item.id}`,
  hidden: Boolean(Number(item.hidden || 0)),
  collects: item.collects != null ? item.collects : item.collect != null ? item.collect : 0,
});

const isVideoFileName = (value: any) => /\.(mp4|mov|m4v|webm|avi|mkv|flv|m3u8)$/i.test(String(value || ''));
const isImageFileName = (value: any) => /\.(jpg|jpeg|png|webp|gif)$/i.test(String(value || ''));

export const normalizeVideoItem = (item: any) => {
  const account = String(item.account || '').trim();
  const rawImage = String(item.image || '').trim().replace(/^\.\.\//, '');
  const videoFile = String(item.videoUrl || item.video || item.mediaUrl || item.file || item.url || '').trim().replace(/^\.\.\//, '');
  let cover = rawImage;

  if (account && cover && !cover.includes('/') && isImageFileName(cover)) {
    cover = `video-cover/${account}/${cover}`;
  } else if (cover.startsWith('user-avatar/')) {
    cover = '';
  }

  return {
    ...item,
    contentType: 'video',
    feedKey: `video-${item.id}`,
    hidden: Boolean(Number(item.hidden || 0)),
    cover,
    videoUrl: videoFile,
    brief: item.brief || '',
    collects: item.collects != null ? item.collects : item.collect != null ? item.collect : 0,
    location: item.location || '',
  };
};
