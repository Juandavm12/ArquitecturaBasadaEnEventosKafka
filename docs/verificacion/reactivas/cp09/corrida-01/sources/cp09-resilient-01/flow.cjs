'use strict';

const { writeFileSync } = require('node:fs');

function once(file, record) {
  writeFileSync(file, `${JSON.stringify(record)}\n`, { flag: 'wx' });
}

async function exercise({ acquire, stop, publish, collect, restore }) {
  await acquire();
  let result;
  let primary;
  let failed = false;
  try {
    await stop();
    const stdout = await publish();
    result = await collect(stdout);
  } catch (error) {
    primary = error;
    failed = true;
  }
  try {
    await restore();
  } catch (restoreError) {
    if (failed) {
      throw new AggregateError([primary, restoreError], 'Flow and restoration failed');
    }
    throw restoreError;
  }
  if (failed) throw primary;
  return result;
}

module.exports = { once, exercise };
