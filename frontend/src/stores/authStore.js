import { create } from 'zustand';
import axios from 'axios';

const useAuthStore = create((set, get) => ({
  authenticated: false,
  checking: true,
  user: null,
  step: 'password',
  sessionId: null,
  maskedEmail: null,
  error: null,
  loading: false,

  checkAuth: async () => {
    set({ checking: true });
    try {
      const res = await axios.get('/api/auth/me');
      if (res.data.authenticated) {
        set({ authenticated: true, user: res.data.user, checking: false });
      } else {
        set({ authenticated: false, user: null, checking: false });
      }
    } catch {
      set({ authenticated: false, user: null, checking: false });
    }
  },

  login: async (password) => {
    set({ loading: true, error: null });
    try {
      const res = await axios.post('/api/auth/login', { password });
      if (res.data.success) {
        set({
          step: '2fa',
          sessionId: res.data.session_id,
          maskedEmail: res.data.masked_email,
          loading: false,
          error: null,
        });
        return true;
      } else {
        set({ error: res.data.error, loading: false });
        return false;
      }
    } catch (e) {
      set({ error: 'Terjadi kesalahan. Coba lagi.', loading: false });
      return false;
    }
  },

  verify2fa: async (code) => {
    const { sessionId } = get();
    set({ loading: true, error: null });
    try {
      const res = await axios.post('/api/auth/verify-2fa', {
        code,
        session_id: sessionId,
      });
      if (res.data.success) {
        set({
          authenticated: true,
          user: 'Dr. Adam M.',
          step: 'password',
          sessionId: null,
          loading: false,
          error: null,
        });
        return true;
      } else {
        set({ error: res.data.error, loading: false });
        return false;
      }
    } catch (e) {
      set({ error: 'Terjadi kesalahan. Coba lagi.', loading: false });
      return false;
    }
  },

  logout: async () => {
    try {
      await axios.post('/api/auth/logout');
    } catch {}
    set({
      authenticated: false,
      user: null,
      step: 'password',
      sessionId: null,
      maskedEmail: null,
      error: null,
    });
  },

  resetLogin: () => {
    set({
      step: 'password',
      sessionId: null,
      maskedEmail: null,
      error: null,
    });
  },
}));

export default useAuthStore;
