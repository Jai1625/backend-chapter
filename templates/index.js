const chatList = document.getElementById('chat-list');
        const chatForm = document.getElementById('chat-form');
        const messageInput = document.getElementById('message-input');
console.log("jaii")
        document.getElementById("userNameButton").addEventListener('click', ()=>{
            let userName = document.getElementById('Name').value;
            console.log("submit name", userName);
            localStorage.setItem("userName", userName);
            window.location.href = "/message";
        })

        // Event listener for chat form submission
        chatForm.addEventListener('submit', (event) => {
            event.preventDefault();
            const message = messageInput.value;
            if (message) {
                sendMessage(message);
                messageInput.value = '';
            }
        });

        // Function to send chat message to server
        function sendMessage(message) {
           let userName = localStorage.getItem("userName");
            let messageInfo = {
                from: userName,
                to: "room",
                message: message
            };
            fetch('/send_message', {
            method: 'POST',
            body:JSON.stringify(messageInfo),
            headers:{'content-type': 'application/json'},
            })
            .then(response => response.json())
            .then(data => console.log(data))
            .catch(errorInfo => console.log(errorInfo, "error"))
        }

        // EventSource to receive SSE events from server
        const eventSource = new EventSource('/chat');
        eventSource.onmessage = (event) => {
            const message = event.data;
            if (message) {
                const listItem = document.createElement('li');
                listItem.textContent = message;
                chatList.appendChild(listItem);
            }
        };