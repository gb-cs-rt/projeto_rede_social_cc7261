let username = ""; // Variable to store the username
let followingUsers = []; // Quem o usuário está seguindo
let lastSeenPostTimestamps = new Set(); // timestamps únicos dos posts já vistos
let followTimestamps = {}; // username → timestamp de quando começou a seguir
let mutualFollows = []; // Usuários com follow mútuo
let currentChatUser = null; // Usuário atualmente aberto no chat

document.getElementById("login-btn").addEventListener("click", async () => {
    const usernameInput = document.getElementById("username-input").value;

    if (usernameInput.trim() === "") {
        alert("Please enter a username.");
        return;
    }

    // Store the username in the variable
    username = usernameInput;

    try {
        const response = await fetch(`http://127.0.0.1:8000/follows/${username}`);
        if (response.ok) {
            followingUsers = await response.json();
        } else {
            console.warn("Não foi possível carregar a lista de follows.");
        }
    } catch (e) {
        console.error("Erro ao carregar follows:", e);
    }

    // Update the welcome title with the username
    document.getElementById("welcome-title").textContent = `Bem-vindo, ${username}`;

    // Hide the login-container and show the post-container and feed
    document.getElementById("login-container").style.display = "none";
    document.getElementById("post-container").style.display = "block";
    document.getElementById("feed").style.display = "block";

    document.getElementById("chat-container").style.display = "flex";

    // 1. Carrega o feed logo após login
    await fetchAndUpdateFeed();

    // 2. Marca os posts já existentes como "vistos" (evita alertas duplicados logo no início)
    try {
        const response = await fetch("http://127.0.0.1:8000/posts");
        if (response.ok) {
            const posts = await response.json();
            posts.forEach(post => lastSeenPostTimestamps.add(post.timestamp));
        }
    } catch (e) {
        console.warn("Falha ao registrar posts existentes como vistos.");
    }

    // 3. Atualizações periódicas com verificação de novos posts
    setInterval(fetchAndUpdateFeed, 5000);
});

document.getElementById("publish-btn").addEventListener("click", async (event) => {
    event.preventDefault(); // Prevent the default form submission behavior

    const postInput = document.getElementById("post-input").value;

    if (postInput.trim() === "") {
        alert("Please enter some content for your post.");
        return;
    }

    // Prepare the post data
    const postData = {
        username: username,
        content: postInput
    };

    try {
        // Send the POST request to the API
        const response = await fetch("http://127.0.0.1:8000/post", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(postData)
        });

        console.log("Response status:", response.status);
        console.log("Response body:", await response.text());

        if (!response.ok) {
            throw new Error("Failed to create post");
        }

        // Clear the input box after successful post
        document.getElementById("post-input").value = "";

        // Optionally, fetch the updated feed
        await fetchAndUpdateFeed();
    } catch (error) {
        console.error("Error creating post:", error);
        alert("Failed to create post. Please try again later.");
    }
});

async function fetchAndUpdateFeed() {
    try {
        const response = await fetch("http://127.0.0.1:8000/posts");
        if (!response.ok) {
            throw new Error("Failed to fetch posts");
        }

        const posts = await response.json();
        updateFeed(posts);

        // Verifica se há novos posts de usuários seguidos
        const newNotifiedTimestamps = [];

        posts.forEach(post => {
            const author = post.username;
            const postTime = post.timestamp;

            const isFollowed = followingUsers.includes(author);
            const followedAfterPost = followTimestamps[author] && postTime <= followTimestamps[author];

            if (
                isFollowed &&
                !followedAfterPost &&
                !lastSeenPostTimestamps.has(postTime)
            ) {
                alert(`Notificação: novo post de @${author}`);
                newNotifiedTimestamps.push(postTime);
            }
        });

        // Atualiza os vistos
        newNotifiedTimestamps.forEach(ts => lastSeenPostTimestamps.add(ts));

    } catch (error) {
        console.error("Error fetching posts:", error);
        alert("Failed to load posts. Please try again later.");
    }

    try {
        const response = await fetch(`http://127.0.0.1:8000/mutual-follows/${username}`);
        if (response.ok) {
            mutualFollows = await response.json();
            updateUserList(mutualFollows);
        } else {
            console.warn("Erro ao buscar mutual follows.");
        }
    } catch (e) {
        console.error("Erro ao carregar mutual follows:", e);
    }
}

function updateFeed(posts) {
    const feed = document.getElementById("feed");
    feed.innerHTML = "";

    // Show most recent posts first
    const sortedPosts = [...posts].sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

    sortedPosts.forEach(post => {
        const postDiv = document.createElement("div");
        postDiv.className = "post";

        const usernameDiv = document.createElement("div");
        usernameDiv.className = "post-username";
        usernameDiv.textContent = `@${post.username}`;

        // Botão de seguir
        if (post.username !== username) {
            const followBtn = document.createElement("button");
            followBtn.className = "follow-btn";

            if (followingUsers.includes(post.username)) {
                followBtn.textContent = "Seguindo";
                followBtn.disabled = true;
            } else {
                followBtn.textContent = "Seguir";
                followBtn.onclick = async () => {
                    try {
                        const response = await fetch("http://127.0.0.1:8000/follow", {
                            method: "POST",
                            headers: {
                                "Content-Type": "application/json"
                            },
                            body: JSON.stringify({
                                follower: username,
                                following: post.username
                            })
                        });
                        if (!response.ok) throw new Error("Erro ao seguir.");

                        alert(`Você está seguindo @${post.username}`);
                        followingUsers.push(post.username);
                        followTimestamps[post.username] = post.timestamp || new Date().toISOString();

                        // Atualiza todos os botões "seguir" do mesmo autor no feed
                        document.querySelectorAll(".post-username").forEach(div => {
                            if (div.textContent === `@${post.username}`) {
                                const btn = div.querySelector("button.follow-btn");
                                if (btn) {
                                    btn.textContent = "Seguindo";
                                    btn.disabled = true;
                                }
                            }
                        });
                    } catch (error) {
                        console.error(error);
                        alert("Erro ao seguir.");
                    }
                };
            }

            usernameDiv.appendChild(followBtn);
        }

        const contentDiv = document.createElement("div");
        contentDiv.className = "post-content";
        contentDiv.textContent = post.content;

        const timestampDiv = document.createElement("div");
        timestampDiv.className = "post-timestamp";
        timestampDiv.textContent = post.timestamp;

        postDiv.appendChild(usernameDiv);
        postDiv.appendChild(contentDiv);
        postDiv.appendChild(timestampDiv);

        feed.appendChild(postDiv);
    });
}

function updateUserList(users) {
    const userList = document.getElementById("user-list");
    userList.innerHTML = "";

    users.forEach(user => {
        const li = document.createElement("li");
        li.textContent = user;
        li.onclick = () => {
            currentChatUser = user;
            if (window.chatInterval) clearInterval(window.chatInterval);
            window.chatInterval = setInterval(() => {
                if (currentChatUser) loadChatHistory(currentChatUser);
            }, 5000);
            document.getElementById("chat-header").textContent = `Chat com @${user}`;
            loadChatHistory(user);
        };
        userList.appendChild(li);
    });
}

async function loadChatHistory(otherUser) {
    try {
        const response = await fetch(`http://127.0.0.1:8000/get-historico/${username}/${otherUser}`);
        if (!response.ok) throw new Error("Erro ao buscar histórico.");

        const messages = await response.json();
        renderMessages(messages);
    } catch (error) {
        console.error(error);
        alert("Erro ao carregar histórico.");
    }
}

function renderMessages(messages) {
    const container = document.getElementById("chat-messages");
    container.innerHTML = "";

    messages.forEach(msg => {
        const div = document.createElement("div");
        div.className = "chat-message " + (msg.sender === username ? "self" : "other");
        div.innerHTML = `
            <span>${msg.content}</span>
            <span class="chat-timestamp">${msg.timestamp}</span>
        `;
        container.appendChild(div);
    });

    container.scrollTop = container.scrollHeight;
}

document.getElementById("chat-send-btn").addEventListener("click", async () => {
    const input = document.getElementById("chat-input");
    const content = input.value.trim();
    if (!content || !currentChatUser) return;

    try {
        const response = await fetch("http://127.0.0.1:8000/enviar-mensagem", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                sender: username,
                receiver: currentChatUser,
                content: content
            })
        });

        if (!response.ok) throw new Error("Erro ao enviar.");

        input.value = "";
        await loadChatHistory(currentChatUser); // recarrega
    } catch (error) {
        console.error(error);
        alert("Erro ao enviar mensagem.");
    }
});

document.getElementById("shutdown-coordinator-btn").addEventListener("click", async () => {
    try {
        const response = await fetch("http://127.0.0.1:8000/shutdown-coordinator", {
            method: "POST"
        });

        if (response.ok) {
            ;
        } else {
            alert("Falha ao solicitar desligamento.");
        }
    } catch (error) {
        console.error(error);
        alert("Erro ao comunicar com o servidor.");
    }
});