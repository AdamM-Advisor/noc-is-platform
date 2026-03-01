import { useState, useRef, useEffect } from 'react';
import { Lock, Shield, ArrowLeft, Eye, EyeOff } from 'lucide-react';
import useAuthStore from '../stores/authStore';

function LoginPage() {
  const { step, maskedEmail, error, loading, login, verify2fa, resetLogin } = useAuthStore();
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [code, setCode] = useState(['', '', '', '', '', '']);
  const inputRefs = useRef([]);

  const handlePasswordSubmit = async (e) => {
    e.preventDefault();
    if (!password.trim()) return;
    await login(password);
  };

  const handleCodeChange = (index, value) => {
    if (!/^\d*$/.test(value)) return;
    const newCode = [...code];
    if (value.length > 1) {
      const digits = value.split('').slice(0, 6 - index);
      digits.forEach((d, i) => {
        if (index + i < 6) newCode[index + i] = d;
      });
      setCode(newCode);
      const nextIdx = Math.min(index + digits.length, 5);
      inputRefs.current[nextIdx]?.focus();
    } else {
      newCode[index] = value;
      setCode(newCode);
      if (value && index < 5) {
        inputRefs.current[index + 1]?.focus();
      }
    }
  };

  const handleCodeKeyDown = (index, e) => {
    if (e.key === 'Backspace' && !code[index] && index > 0) {
      inputRefs.current[index - 1]?.focus();
    }
  };

  useEffect(() => {
    const fullCode = code.join('');
    if (fullCode.length === 6) {
      verify2fa(fullCode).then(success => {
        if (!success) setCode(['', '', '', '', '', '']);
      });
    }
  }, [code]);

  const handleBack = () => {
    setPassword('');
    setCode(['', '', '', '', '', '']);
    resetLogin();
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-[#0F172A] via-[#1B2A4A] to-[#1E3A5F] flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-white/10 backdrop-blur-sm mb-4">
            <Shield size={32} className="text-white" />
          </div>
          <h1 className="text-2xl font-bold text-white tracking-wide">NOC-IS Analytics Platform</h1>
          <p className="text-sm text-gray-400 mt-1">Secured Access</p>
        </div>

        <div className="bg-white rounded-2xl shadow-2xl overflow-hidden">
          {step === 'password' ? (
            <div className="p-8">
              <div className="text-center mb-6">
                <div className="inline-flex items-center justify-center w-12 h-12 rounded-full bg-[#EEF2FF] mb-3">
                  <Lock size={24} className="text-[#1E40AF]" />
                </div>
                <h2 className="text-lg font-semibold text-[#0F172A]">Masuk ke Platform</h2>
                <p className="text-sm text-[#475569] mt-1">Masukkan password untuk melanjutkan</p>
              </div>

              <form onSubmit={handlePasswordSubmit}>
                <div className="mb-4">
                  <label className="block text-xs font-medium text-[#475569] mb-1.5">Password</label>
                  <div className="relative">
                    <input
                      type={showPassword ? 'text' : 'password'}
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      className="w-full px-4 py-3 border border-gray-300 rounded-lg text-sm text-[#0F172A] focus:outline-none focus:ring-2 focus:ring-[#1E40AF] focus:border-transparent transition-all"
                      placeholder="Masukkan password"
                      autoFocus
                      disabled={loading}
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-3 top-1/2 -translate-y-1/2 text-[#94A3B8] hover:text-[#475569]"
                    >
                      {showPassword ? <EyeOff size={18} /> : <Eye size={18} />}
                    </button>
                  </div>
                </div>

                {error && (
                  <div className="mb-4 px-3 py-2 bg-red-50 border border-red-200 rounded-lg text-sm text-red-600">
                    {error}
                  </div>
                )}

                <button
                  type="submit"
                  disabled={loading || !password.trim()}
                  className="w-full py-3 bg-[#1E40AF] text-white rounded-lg font-medium text-sm hover:bg-[#1E3A8A] disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  {loading ? (
                    <span className="flex items-center justify-center gap-2">
                      <span className="animate-spin w-4 h-4 border-2 border-white border-t-transparent rounded-full" />
                      Memverifikasi...
                    </span>
                  ) : (
                    'Lanjutkan'
                  )}
                </button>
              </form>
            </div>
          ) : (
            <div className="p-8">
              <div className="text-center mb-6">
                <div className="inline-flex items-center justify-center w-12 h-12 rounded-full bg-[#EEF2FF] mb-3">
                  <Shield size={24} className="text-[#1E40AF]" />
                </div>
                <h2 className="text-lg font-semibold text-[#0F172A]">Verifikasi 2FA</h2>
                <p className="text-sm text-[#475569] mt-1">
                  Kode verifikasi dikirim ke <span className="font-medium text-[#0F172A]">{maskedEmail}</span>
                </p>
              </div>

              <div className="flex justify-center gap-2 mb-4">
                {code.map((digit, i) => (
                  <input
                    key={i}
                    ref={el => inputRefs.current[i] = el}
                    type="text"
                    inputMode="numeric"
                    maxLength={i === 0 ? 6 : 1}
                    value={digit}
                    onChange={(e) => handleCodeChange(i, e.target.value)}
                    onKeyDown={(e) => handleCodeKeyDown(i, e)}
                    className="w-12 h-14 text-center text-xl font-bold border-2 border-gray-300 rounded-lg text-[#0F172A] focus:outline-none focus:ring-2 focus:ring-[#1E40AF] focus:border-[#1E40AF] transition-all"
                    autoFocus={i === 0}
                    disabled={loading}
                  />
                ))}
              </div>

              {error && (
                <div className="mb-4 px-3 py-2 bg-red-50 border border-red-200 rounded-lg text-sm text-red-600 text-center">
                  {error}
                </div>
              )}

              {loading && (
                <div className="flex items-center justify-center gap-2 mb-4 text-sm text-[#475569]">
                  <span className="animate-spin w-4 h-4 border-2 border-[#1E40AF] border-t-transparent rounded-full" />
                  Memverifikasi kode...
                </div>
              )}

              <p className="text-xs text-[#94A3B8] text-center mb-4">
                Kode berlaku selama 5 menit
              </p>

              <button
                onClick={handleBack}
                className="w-full flex items-center justify-center gap-2 py-2.5 text-sm text-[#475569] hover:text-[#0F172A] transition-colors"
              >
                <ArrowLeft size={16} />
                Kembali ke halaman login
              </button>
            </div>
          )}

          <div className="bg-[#F8FAFC] border-t border-gray-100 px-8 py-3">
            <p className="text-xs text-[#94A3B8] text-center">
              NOC-IS Analytics Platform v1.0 — Dr. Adam M.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default LoginPage;
