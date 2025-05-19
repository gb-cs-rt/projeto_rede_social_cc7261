let username = ""; // Variable to store the username
let followingUsers = []; // Quem o usuário está seguindo
let lastSeenPostTimestamps = new Set(); // timestamps únicos dos posts já vistos
let followTimestamps = {}; // username → timestamp de quando começou a seguir

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
    setInterval(fetchAndUpdateFeed, 10000);
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
}

function updateFeed(posts) {
    const feed = document.getElementById("feed");
    feed.innerHTML = "";

    posts.forEach(post => {
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
