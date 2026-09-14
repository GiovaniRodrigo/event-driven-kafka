import * as React from 'react';

export type Lang = 'pt' | 'en';

/**
 * Lightweight in-app i18n. Portuguese-first (matching the v0 design), with an
 * in-header toggle to English. No server, no persistence beyond the session.
 * `t(key)` resolves a dot-key against the active dictionary, falling back to the
 * key itself so a missing string is visible rather than blank.
 */
const DICT: Record<Lang, Record<string, string>> = {
  pt: {
    'shell.brand': 'Streamline',
    'shell.brandSub': 'Kafka operations',
    'shell.env': 'produção',
    'shell.cluster': 'us-east-1 / cluster-a',
    'shell.workspace': 'WORKSPACE',
    'shell.platform': 'Order platform',
    'shell.nav': 'Navegação principal',
    'shell.search': 'Pesquisar',
    'shell.profile': 'Perfil',
    'shell.language': 'Idioma',
    'shell.theme': 'Alternar tema',
    'shell.menu': 'Menu',
    'shell.live': 'Ativo',
    'shell.offline': 'Offline',
    'shell.liveOn': 'Conexão ao vivo ativa',
    'shell.liveOff': 'Conexão ao vivo offline',
    'shell.newOrder': 'Novo pedido',
    'shell.soon': 'em breve',
    'shell.footer': 'Design system v2.4.0',

    'nav.overview': 'Visão geral',
    'nav.orders': 'Pedidos',
    'nav.consumers': 'Consumers',
    'nav.map': 'Mapa de requisições',

    'overview.recentOrders': 'Pedidos recentes',
    'overview.consumerHealth': 'Saúde dos consumers',
    'metric.total': 'Total de pedidos',
    'metric.completed': 'Concluídos',
    'metric.failed': 'Falhos',
    'metric.avg': 'Ticket médio',

    'consumers.title': 'Consumers',
    'consumers.subtitle': 'Saúde de cada etapa do pipeline, transmitida ao vivo via Socket.IO.',
    'consumers.service': 'Serviço',
    'consumers.status': 'Status',
    'consumers.database': 'Banco de dados',
    'consumers.version': 'versão',
    'consumers.unknown': 'desconhecido',

    'order.back': 'Voltar à visão geral',
    'order.order': 'Pedido',
    'order.created': 'criado',
    'order.total': 'Total',
    'order.items': 'Itens',
    'order.noItems': 'Nenhum item registrado.',
    'order.timeline': 'Linha do tempo de eventos',
    'order.notFound': 'Pedido não encontrado',
    'order.notFoundBody': 'Nenhum pedido corresponde ao id',
    'order.loadError': 'Não foi possível carregar o pedido',

    'error.metrics': 'Métricas indisponíveis',
    'error.orders': 'Pedidos indisponíveis',
    'error.ordersBody': 'Não foi possível acessar o backend. Verifique se a API está rodando.',
    'error.health': 'Saúde indisponível',
    'error.backendUnreachable': 'O backend está inacessível.',
  },
  en: {
    'shell.brand': 'Streamline',
    'shell.brandSub': 'Kafka operations',
    'shell.env': 'production',
    'shell.cluster': 'us-east-1 / cluster-a',
    'shell.workspace': 'WORKSPACE',
    'shell.platform': 'Order platform',
    'shell.nav': 'Primary navigation',
    'shell.search': 'Search',
    'shell.profile': 'Profile',
    'shell.language': 'Language',
    'shell.theme': 'Toggle theme',
    'shell.menu': 'Menu',
    'shell.live': 'Live',
    'shell.offline': 'Offline',
    'shell.liveOn': 'Live connection active',
    'shell.liveOff': 'Live connection offline',
    'shell.newOrder': 'New order',
    'shell.soon': 'soon',
    'shell.footer': 'Design system v2.4.0',

    'nav.overview': 'Overview',
    'nav.orders': 'Orders',
    'nav.consumers': 'Consumers',
    'nav.map': 'Request map',

    'overview.recentOrders': 'Recent orders',
    'overview.consumerHealth': 'Consumer health',
    'metric.total': 'Total orders',
    'metric.completed': 'Completed',
    'metric.failed': 'Failed',
    'metric.avg': 'Avg order value',

    'consumers.title': 'Consumers',
    'consumers.subtitle': 'Health of each pipeline stage, broadcast live over Socket.IO.',
    'consumers.service': 'Service',
    'consumers.status': 'Status',
    'consumers.database': 'Database',
    'consumers.version': 'v',
    'consumers.unknown': 'unknown',

    'order.back': 'Back to overview',
    'order.order': 'Order',
    'order.created': 'created',
    'order.total': 'Total',
    'order.items': 'Items',
    'order.noItems': 'No items recorded.',
    'order.timeline': 'Event timeline',
    'order.notFound': 'Order not found',
    'order.notFoundBody': 'No order matches id',
    'order.loadError': 'Could not load order',

    'error.metrics': 'Metrics unavailable',
    'error.orders': 'Orders unavailable',
    'error.ordersBody': 'Could not reach the backend. Check that the API is running.',
    'error.health': 'Health unavailable',
    'error.backendUnreachable': 'The backend is unreachable.',
  },
};

export type TFn = (key: string) => string;

interface I18nContextValue {
  lang: Lang;
  setLang: (l: Lang) => void;
  toggle: () => void;
  t: TFn;
}

const I18nContext = React.createContext<I18nContextValue | null>(null);

export function I18nProvider({
  children,
  initialLang = 'pt',
}: {
  children: React.ReactNode;
  initialLang?: Lang;
}) {
  const [lang, setLang] = React.useState<Lang>(initialLang);

  React.useEffect(() => {
    document.documentElement.lang = lang;
  }, [lang]);

  const value = React.useMemo<I18nContextValue>(
    () => ({
      lang,
      setLang,
      toggle: () => setLang((l) => (l === 'pt' ? 'en' : 'pt')),
      t: (key: string) => DICT[lang][key] ?? key,
    }),
    [lang],
  );

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n(): I18nContextValue {
  const ctx = React.useContext(I18nContext);
  if (!ctx) throw new Error('useI18n must be used within <I18nProvider>');
  return ctx;
}
