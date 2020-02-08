# Build /dist
FROM node:12-alpine
WORKDIR /app
COPY package.json yarn.lock ./
RUN yarn
COPY . /app
ENTRYPOINT ["yarn","start"]
