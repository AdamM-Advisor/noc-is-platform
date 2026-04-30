import axios from 'axios';

export const API_ORIGIN = (import.meta.env.VITE_API_ORIGIN || '').replace(/\/$/, '');
export const API_BASE_URL = API_ORIGIN ? `${API_ORIGIN}/api` : '/api';

axios.defaults.baseURL = API_ORIGIN || '';
axios.defaults.withCredentials = true;
