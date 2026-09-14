const LOG_LEVEL = process.env.LOG_LEVEL || 'info';

const levels: { [key: string]: number } = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

function shouldLog(level: string): boolean {
  return levels[level] >= levels[LOG_LEVEL];
}

function formatLog(level: string, data: any): string {
  return JSON.stringify({
    timestamp: new Date().toISOString(),
    level: level.toUpperCase(),
    ...data,
  });
}

export const logger = {
  debug: (data: any) => {
    if (shouldLog('debug')) {
      console.log(formatLog('debug', data));
    }
  },

  info: (data: any) => {
    if (shouldLog('info')) {
      console.log(formatLog('info', data));
    }
  },

  warn: (data: any) => {
    if (shouldLog('warn')) {
      console.warn(formatLog('warn', data));
    }
  },

  error: (data: any) => {
    if (shouldLog('error')) {
      console.error(formatLog('error', data));
    }
  },
};
