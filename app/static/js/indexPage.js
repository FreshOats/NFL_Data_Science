// Slide Show
let slideIndex = 1;
showSlides(slideIndex);

function plusSlides(n) {
    showSlides(slideIndex += n);
}

function showSlides(n) {
    let i;
    let slides = document.getElementsByClassName("slide");
    if (n > slides.length) { slideIndex = 1 }
    if (n < 1) { slideIndex = slides.length }
    for (i = 0; i < slides.length; i++) {
        slides[i].style.display = "none";
    }
    slides[slideIndex - 1].style.display = "block";
}


// Injury table function
function populateInjuryTable() {
    fetch('/api/injuries')
        .then(response => response.json())
        .then(data => {
            const tableBody = document.querySelector('#injury-table tbody');
            tableBody.innerHTML = '';

            data.slice(0, 15).forEach(injury => {
                const row = tableBody.insertRow();
                row.insertCell(0).textContent = injury.Team;
                row.insertCell(1).textContent = injury.Player;
                row.insertCell(2).textContent = injury.Position;
                row.insertCell(3).textContent = injury.Injury;
            });
        })
        .catch(error => console.error('Error fetching injury data:', error));
}

// Initialize functions when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    showSlides(1); // Initialize the slideshow
    populateInjuryTable(); // Populate the injury table
});
