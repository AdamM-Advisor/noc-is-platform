import axios from 'axios';
import { API_BASE_URL } from './axiosConfig';

const client = axios.create({
  baseURL: API_BASE_URL,
  timeout: 60000,
  withCredentials: true,
});

const uploadClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 300000,
  withCredentials: true,
});

const showToast = (message) => {
  const existing = document.querySelector('.toast-notification');
  if (existing) existing.remove();

  const toast = document.createElement('div');
  toast.className = 'toast-notification';
  toast.style.cssText = `
    position: fixed; top: 20px; right: 20px; z-index: 9999;
    background: #DC2626; color: white; padding: 12px 20px;
    border-radius: 8px; font-size: 14px; max-width: 400px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    animation: slideIn 0.3s ease;
  `;
  toast.textContent = message;
  document.body.appendChild(toast);
  setTimeout(() => toast.remove(), 5000);
};

const errorInterceptor = (error) => {
  if (error.response) {
    const status = error.response.status;
    if (status === 413) {
      showToast('File terlalu besar. Gunakan chunked upload.');
    } else if (status >= 500) {
      showToast('Server error. Coba lagi.');
    } else if (error.response.data?.detail) {
      showToast(error.response.data.detail);
    }
  } else if (error.request) {
    showToast('Tidak dapat terhubung ke server.');
  }
  return Promise.reject(error);
};

client.interceptors.response.use((r) => r, errorInterceptor);
uploadClient.interceptors.response.use((r) => r, errorInterceptor);

export { client, uploadClient };
export default client;
