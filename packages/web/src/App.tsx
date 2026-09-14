import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import { AppLayout } from '@/routes/AppLayout';
import { Overview } from '@/routes/Overview';
import { OrderDetail } from '@/routes/OrderDetail';
import { Consumers } from '@/routes/Consumers';

const router = createBrowserRouter([
  {
    element: <AppLayout />,
    children: [
      { path: '/', element: <Overview /> },
      { path: '/orders/:id', element: <OrderDetail /> },
      { path: '/consumers', element: <Consumers /> },
    ],
  },
]);

export function App() {
  return <RouterProvider router={router} />;
}
