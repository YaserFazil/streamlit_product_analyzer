const socket = io(); //socketio connection to server//
console.log("Hello")
socket.on("connect", () => {
    console.log("connected");
    document.getElementById("header").innerHTML = "<h3>" + "Websocket Connected" + "</h3";


});

socket.on("disconnect", () => {
    console.log("disconnected");
    document.getElementById("header").innerHTML = "<h3>" + "Websocket Disconnected" + "</h3>";
});

function myupdate() {
    //Event sent by Client
    socket.emit("my_event")
}

// Event sent by Server//
socket.on("server", function(msg) {
    document.getElementById("terminal_content").innerHTML += msg.data + "<br>";
    document.getElementById("checkbutton").disabled = true;
    document.getElementById("checkbutton").innerHTML = "Loading..";
    document.getElementById("checkbutton").style.cursor = "not-allowed";
    document.getElementById("checkbutton").style.pointerEvents = "auto";
    scrollToBottom()

});



// Function to scroll the terminal to the bottom
function scrollToBottom() {
    var terminalBody = document.getElementById('terminal__body');
    terminalBody.scrollTop = terminalBody.scrollHeight; // Scroll to the bottom
};