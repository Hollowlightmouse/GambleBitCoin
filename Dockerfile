FROM node:18-alpine

WORKDIR /app

RUN apk add --no-cache python3 py3-pip
RUN pip3 install --no-cache-dir python-binance kafka-python

COPY package*.json ./
RUN npm install --production

COPY . .

EXPOSE 3000

CMD ["node", "app.js"]
