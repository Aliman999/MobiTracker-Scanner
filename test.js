const Bottleneck = require('bottleneck');
const limiter = new Bottleneck();

// Listen to the "failed" event
limiter.on("failed", async (error, jobInfo) => {
  const id = jobInfo.options.id;
  console.warn(`Job ${id} failed: ${error}`);

  if (jobInfo.retryCount === 0) { // Here we only retry once
    console.log(`Retrying job ${id} in 25ms!`);
    return 25;
  }
});

// Listen to the "retry" event
limiter.on("retry", (error, jobInfo) => console.log(`Now retrying ${jobInfo.options.id}`));

const main = async function () {
  let executions = 0;

  // Schedule one job
  const result = await limiter.schedule({ id: 'ABC123' }, async () => {
    executions++;
    if (executions === 1) {
      throw new Error("Boom!");
    } else {
      return "Success!";
    }
  });

  console.log(`Result: ${result}`);
}

main();
