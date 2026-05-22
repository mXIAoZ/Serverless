exports.handler = async function handler(event, context) {
  const name = event && event.name ? event.name : 'world';
  return {
    statusCode: 200,
    message: `Hello, ${name}!`,
    requestId: context.awsRequestId,
  };
};
