const { MongoClient, ObjectId } = require('mongodb')


function isObjectId (str) {
  return str.match(/^[0-9a-fA-F]{24}$/)
}

/**
 * Класс очереди событий
 * Сохранение событий в источнике событий будет надежнее черем передавать события по http
 * так как сервер может быть не доступен (например сбой или перезагрузка)
 * MongoDB достаточно надежна так как используется реплика
 */
class GrattisEvent {
  mongoURL = ''
  dbName = ''
  eventCollectionName = ''
  mongoClient = null
  db = null
  collection = null
  isRun = false

  /**
   * Создание экземпляра
   * @param {string} mongoURL - Строка подключения к MongoDB
   * @param {string} dbName - Имя базы данных к которой надо подключиться
   * @param {string} eventCollectionName - Имя коллекции в которую будет сохранять события
   */
  constructor (mongoURL, eventCollectionName = 'Event', dbName = 'grattis') {
    this.mongoURL = mongoURL
    this.dbName = dbName
    this.eventCollectionName = eventCollectionName
  }

  /**
   * Подключение в базе данных
   * @returns {Promise<void>}
   */
  async connect() {
    this.mongoClient = new MongoClient(this.mongoURL)
    await this.mongoClient.connect()
    this.db = this.mongoClient.db(this.dbName)
    this.collection = this.db.collection(this.eventCollectionName)
  }

  /**
   * Сохранение события в очереди (в MongoDB)
   * @param {object} value - данные события
   * @param {number} date - дата наступления события, можно передать отложенную дату
   * @returns {Promise<void>}
   */
  async send (value, date = Date.now()) {
    if (!this.collection) {
      await this.connect()
    }

    Object.keys(value).forEach(key => {
      const val = value[key]
      if (typeof val === 'string') {
        return
      }
      if (isObjectId(val)) {
        value[key] = new ObjectId(val)
      }
    })

    await this.collection.insert({
      value,
      date,
      // флаги отвечающие за обработку очереди
      isWork: false,
      isDone: false
    })
  }

  /**
   * Метод получения сразу пачки событий
   * Он должен работать в одном экзепляре в одном процессе иначе будует дублирование обработки событий
   * Список событий отсортирован в порядке возрастания (первый элемент самый старый)
   * @param {function} cb - Функция в которую передается результат (должна быть async)
   * @param {number} timeout - Как часто запрашиваем пачку данных (по умолчанию раз в минуту)
   * @param {number} limit - Размер пачки (количество событий за раз)
   * @returns {Promise<void>}
   */
  async getItems (cb, timeout = 1000 * 60, limit = 10000) {
    if (!this.collection) {
      await this.connect()
    }

    setInterval(async () => {
      try {
        // Если функция уже запущена пропускаем итерацию
        if (this.isRun) return null
        // Ставим что запустился процесс
        this.isRun = true

        // Получаем события
        const events = await this.collection.find(
          {
            date: {
              $lt: Date.now()
            },
            isWork: false, // не в работе
            isDone: false, // не завершен
          }).sort({ date: 1 }).limit(limit).toArray()

        if (!events || events.length === 0) return null

        // отправляем события в функцию
        await cb(events)
        // статим что мы обработали всю пачку
        this.isRun = false
      } catch (err) {
        console.log(err)
        // чтобы не залипать в случае ошибки ставим что нифига не запущено
        this.isRun = false
      }
    }, timeout)
  }

  /**
   * Удаление события
   * @param eventId
   * @returns {Promise<void>}
   */
  async removeEvent (eventId) {
    await this.collection.deleteOne({
      _id: new ObjectId(eventId)
    })
  }
}

module.exports = GrattisEvent
