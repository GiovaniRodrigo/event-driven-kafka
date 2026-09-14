import { createBrowserRouter, RouterProvider, type RouteObject } from 'react-router-dom';
import { AppLayout } from '@/routes/AppLayout';
import { Overview } from '@/routes/Overview';
import { OrderDetail } from '@/routes/OrderDetail';
import { Consumers } from '@/routes/Consumers';

/** Route table, exported so tests can mount it with a memory router. */
export const routes: RouteObject[] = [
  {
    element: <AppLayout />,
    children: [
      { path: '/', element: <Overview /> },
      { path: '/orders/:id', element: <OrderDetail /> },
      { path: '/consumers', element: <Consumers /> },
    ],
  },
];

const router = createBrowserRouter(routes);

export function App() {
  return <RouterProvider router={router} />;
}
