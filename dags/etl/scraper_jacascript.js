/**
 * scraper-javascript.js
 * To run this script, copy and paste `node scraper-javascript.js` in the terminal
 */

const cheerio = require('cheerio');



(async () => {
  const url = 'https://www.example.com';
  const response = await fetch(url);

  const $ = cheerio.load(await response.text());
  console.log($.html());

})();