const express = require('express')
const axios = require('axios')
const bodyParser = require("body-parser");

const app = express();
const port = 3000;

app.use(bodyParser.json());

// ✅ Support plain text bodies (for SNS)
app.use(bodyParser.text({ type: 'text/plain' }));


app.post('/sns', async (req, res) => {
    let snsMessage;
    const msgType = req.headers['x-amz-sns-message-type'];
    console.log('📩 Headers:', req.headers);

    try {
        // Parse the raw text body (SNS sends it as text/plain)
        snsMessage = JSON.parse(req.body);
        console.log('📨 Body:', snsMessage);
    } catch (e) {
        console.error('❌ Failed to parse SNS message body:', e);
        return res.sendStatus(400);
    }

    if (msgType === 'SubscriptionConfirmation') {
        const subscribeURL = snsMessage.SubscribeURL;
        console.log('🔔 Confirming subscription with URL:', subscribeURL);

        try {
            await axios.get(subscribeURL);
            console.log('✅ Subscription confirmed!');
        } catch (err) {
            console.error('❌ Subscription confirmation failed:', err);
        }
    }

    if (msgType === 'Notification') {
        console.log('🔔 Message:', snsMessage.Message);
    }

    res.sendStatus(200);
});

app.listen(port, () => {
    console.log(`🚀 SNS listener running at http://localhost:${port}/sns`);
})