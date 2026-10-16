FROM node:20-bookworm-slim

WORKDIR /usr/src/app

COPY package.json package-lock.json ./

RUN npm install
RUN npm install pm2 -g

COPY . .
RUN npm run build

EXPOSE 8888

CMD ["pm2-runtime", "dist/index.js", "--name", "saga_master"]
