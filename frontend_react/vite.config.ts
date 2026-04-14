import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  // MÁGICA CONTRA O CORS AQUI:
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000', // Onde o Python está rodando
        changeOrigin: true,
        secure: false,
      }
    }
  }
})