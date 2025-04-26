let username = ""; // Variable to store the username

document.getElementById("login-btn").addEventListener("click", async () => {
    const usernameInput = document.getElementById("username-input").value;

    if (usernameInput.trim() === "") {
        alert("Please enter a username.");
        return;
    }

    // Store the username in the variable
    username = usernameInput;

    // Update the welcome title with the username
    document.getElementById("welcome-title").textContent = `Bem-vindo, ${username}`;

    // Hide the login-container and show the post-container and feed
    document.getElementById("login-container").style.display = "none";
    document.getElementById("post-container").style.display = "block";
    document.getElementById("feed").style.display = "block";

    // Fetch posts immediately and start periodic updates
    await fetchAndUpdateFeed();
    setInterval(fetchAndUpdateFeed, 10000); // Fetch posts every 10 seconds
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
    } catch (error) {
        console.error("Error fetching posts:", error);
        alert("Failed to load posts. Please try again later.");
    }
}

function updateFeed(posts) {
    const feed = document.getElementById("feed");
    feed.innerHTML = ""; // Clear the feed

    posts.forEach(post => {
        const postDiv = document.createElement("div");
        postDiv.className = "post";

        const usernameDiv = document.createElement("div");
        usernameDiv.className = "post-username";
        usernameDiv.textContent = `@${post.username}`;

        const contentDiv = document.createElement("div");
        contentDiv.className = "post-content";
        contentDiv.textContent = post.content;

        const timestampDiv = document.createElement("div");
        timestampDiv.className = "post-timestamp";
        timestampDiv.textContent = post.timestamp;

        // Append elements to the post container
        postDiv.appendChild(usernameDiv);
        postDiv.appendChild(contentDiv);
        postDiv.appendChild(timestampDiv);

        // Add the post to the feed
        feed.appendChild(postDiv);
    });
}