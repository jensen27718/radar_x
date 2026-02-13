# radar-x

Bot de Telegram para navegar y filtrar contenido de `SITE_BASE_URL` (por defecto: `https://doncolombia.com`) con alertas automaticas por usuario.

## Seguridad (importante)

No pegues ni comitees el token del bot en el repo. Usa `wrangler secret`.

Si el token ya fue compartido en publico, revocalo y genera uno nuevo con BotFather.

## Funciones

- Filtros por usuario:
  - Suscripcion a foros (por URL o desde `/foros`)
  - Palabras clave (incluir / excluir)
  - Prefijos/labels (ej: "Reseña", "Pregunta", etc, si el sitio los expone en HTML)
- Notificaciones automaticas:
  - Nuevo tema (thread)
  - Nueva respuesta (reply) en foros suscritos (best-effort, basado en timestamps del HTML)
- Comandos para navegar:
  - `/latest` muestra los ultimos resultados con tus filtros

## Comandos del bot

- `/start`
- `/help`
- `/foros` (lista con botones para suscribirte / quitar)
- `/addforum <url>` o `/addforo <url>`
- `/rmforum <id>` o `/rmforo <id>`
- `/addkw <texto>` / `/rmkw <texto>`
- `/addnot <texto>` / `/rmnot <texto>`
- `/addprefix <texto>` / `/rmprefix <texto>`
- `/notify threads on|off`
- `/notify replies on|off`
- `/list` (o `/filtros`)
- `/latest` (o `/ultimos`)

## Despliegue (Cloudflare Workers + KV + Cron)

Requisitos:

- Node.js + npm
- Cuenta de Cloudflare

Pasos:

1. Instalar dependencias:

```powershell
npm install
```

2. Login en Cloudflare:

```powershell
npx wrangler login
```

3. Crear un KV namespace y pegar los IDs en `wrangler.toml`:

```powershell
npx wrangler kv namespace create "radar_x_kv"
npx wrangler kv namespace create "radar_x_kv_preview" --preview
```

4. Cargar secretos:

```powershell
npx wrangler secret put TELEGRAM_BOT_TOKEN
npx wrangler secret put TELEGRAM_WEBHOOK_SECRET_TOKEN
```

5. Deploy:

```powershell
npm run deploy
```

6. Configurar el webhook de Telegram:

```powershell
$token = "<TELEGRAM_BOT_TOKEN>"
$secret = "<TELEGRAM_WEBHOOK_SECRET_TOKEN>"
$hook = "https://<TU-WORKER>.workers.dev/telegram/webhook"

Invoke-RestMethod -Method Post `
  -Uri "https://api.telegram.org/bot$token/setWebhook" `
  -ContentType "application/json" `
  -Body (@{ url = $hook; secret_token = $secret } | ConvertTo-Json)
```

## Notas

- El cron esta configurado en `wrangler.toml` cada 5 minutos (`*/5 * * * *`).
- La primera ejecucion "primea" estado y no manda spam historico. Para ver contenido inmediato usa `/latest`.

