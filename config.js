require('dotenv').config();

const config = {
  MysqlHost:process.env.MYSQL_HOST,
  MysqlUsername:process.env.MYSQL_USERNAME,
  MysqlPassword:process.env.MYSQL_PASSWORD,
  MysqlDatabase:process.env.MYSQL_DATABASE,
  Key:process.env.KEY,
  Secret:process.env.SECRET
};
module.exports = config;
