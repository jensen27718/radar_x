# radar-x

Bot de Telegram enfocado en **resenas**: crear, buscar, votar y opinar, todo con botones (wizard).

Como respaldo (cuando aun hay pocas resenas), tambien filtra y manda contenido de `SITE_BASE_URL` (por defecto: `https://doncolombia.com`).

## Seguridad (importante)

No pegues ni comitees el token del bot en el repo. Usa `wrangler secret`.

Si el token ya fue compartido en publico, revocalo y genera uno nuevo con BotFather.

## Funciones

- Resenas (principal):
  - Wizard para crear resenas (modo rapido o completo tipo DonColombia)
  - Buscar resena por nombre/apodo, telefono, link o ID
  - Interaccion: votos "Buena/Mala" + opiniones (comentarios)
  - Pagina publica por resena: `/r/<id>` (para abrir desde Telegram)
  - IA opcional (DeepSeek) para mejorar el relato del "Comentario general" (sin inventar datos)
- Filtros por usuario:
  - Suscripcion a foros (por URL o desde `/foros`)
  - Palabras clave (incluir / excluir)
  - Prefijos/labels (ej: "Resena", "Pregunta", etc, si el sitio los expone en HTML)
- Notificaciones automaticas (feed):
  - Envios en tiempos aleatorios por usuario (entre 5 y 60 min)
  - Prioriza resenas propias; si no hay, manda contenido de DonColombia (y si no hay nada nuevo, manda cosas mas viejas)
  - Detecta: nuevo tema (thread) y nueva respuesta (reply) en foros/home (best-effort)
- Donaciones (Wompi):
  - Landing `/donar` estilo "vaquita" con meta mensual y botones de $1k/$2k/$5k/$10k
  - Webhook `/wompi/webhook` para sumar aportes aprobados (APPROVED)
- Lectura:
  - Boton "Leer" abre una pagina `/leer?u=...` (lector simple con imagen si existe)
  - `/leer <url>` devuelve titulo + extracto del contenido (tambien funciona si pegas un link directo)

## Comandos del bot

- Recomendado: usa `/start` y sigue el wizard por botones (ciudades y/o foros). Con eso ya queda prendido el feed.
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
- `/notify reviews on|off`
- `/notify donations on|off`
- `/list` (o `/filtros`)
- `/leer <url>` (o `/read <url>`)
- `/cancel` (cancelar resena/opinion/busqueda en curso)

## Despliegue (Cloudflare Workers + KV + Cron)

Requisitos:

- Node.js + npm
- Cuenta de Cloudflare

Pasos:

1. Instalar dependencias:

```powershell
npm.cmd install
```

2. Login en Cloudflare:

```powershell
npx.cmd wrangler login
```

3. Crear un KV namespace y pegar los IDs en `wrangler.toml`:

```powershell
npx.cmd wrangler kv namespace create "radar_x_kv"
npx.cmd wrangler kv namespace create "radar_x_kv_preview" --preview
```

4. Cargar secretos:

```powershell
npx.cmd wrangler secret put TELEGRAM_BOT_TOKEN
npx.cmd wrangler secret put TELEGRAM_WEBHOOK_SECRET_TOKEN

# Wompi (donaciones)
npx.cmd wrangler secret put WOMPI_INTEGRITY_SECRET
npx.cmd wrangler secret put WOMPI_EVENTS_SECRET

# DeepSeek (opcional, para mejorar el relato de resenas)
npx.cmd wrangler secret put DEEPSEEK_API_KEY
```

5. Deploy:

```powershell
npm.cmd run deploy
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

- Si es la primera vez que usas Workers en esa cuenta, registra tu subdominio `workers.dev` en el dashboard de Cloudflare (Workers & Pages) antes del primer deploy.
- El cron esta configurado en `wrangler.toml` cada 5 minutos (`*/5 * * * *`).
- La primera ejecucion "primea" estado y no manda spam historico.

## Config Wompi (resumen)

- Variables en `wrangler.toml`:
  - `PUBLIC_BASE_URL` (la URL publica del Worker, para links en botones)
  - `WOMPI_PUBLIC_KEY` (no es secreta)
  - `DONATION_MONTHLY_GOAL_CENTS` (por defecto 300.000 COP => `30000000`)
  - `DONATION_REFERENCE_PREFIX` (por defecto `radarx`)
- Webhook:
  - En Wompi configura un webhook apuntando a: `https://<TU-WORKER>.workers.dev/wompi/webhook`
  - El Worker valida el checksum con `WOMPI_EVENTS_SECRET` y solo suma transacciones `APPROVED` cuyo `reference` empiece por `DONATION_REFERENCE_PREFIX`.
