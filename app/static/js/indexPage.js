// Slide Show
let slideIndex = 1;
showSlides(slideIndex);

function plusSlides(n) {
    showSlides(slideIndex += n);
}

function showSlides(n) {
    let slides = document.getElementsByClassName("slide");
    if (n > slides.length) { slideIndex = 1 }
    if (n < 1) { slideIndex = slides.length }
    for (let i = 0; i < slides.length; i++) {
        slides[i].style.display = "none";
    }
    slides[slideIndex - 1].style.display = "block";
    adjustSlideshow();
}

function adjustSlideshow() {
    const slideshow = document.querySelector('.slideshow-container');
    const currentSlide = slideshow.querySelector(`.slide:nth-child(${slideIndex})`);
    const currentImage = currentSlide.querySelector('img');

    // Reset the slideshow height to auto to get the natural image height
    slideshow.style.height = 'auto';

    // Use setTimeout to ensure the image has loaded
    setTimeout(() => {
        const imageHeight = currentImage.offsetHeight;
        slideshow.style.height = `${imageHeight}px`;
        adjustTableHeight();
    }, 0);
}

function adjustTableHeight() {
    const slideshow = document.querySelector('.slideshow-container');
    const tableContainer = document.querySelector('.injury-table-container');
    tableContainer.style.height = `${slideshow.offsetHeight}px`;
}

function populateInjuryTable() {
    console.log('Attempting to fetch injury data...');
    fetch('/api/injuries')
        .then(response => {
            console.log('Response received:', response);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Parsed data:', data);
            const tableBody = document.querySelector('#injury-table tbody');
            if (!tableBody) {
                throw new Error('Table body not found!');
            }
            tableBody.innerHTML = '';  // Clear existing content

            if (!Array.isArray(data) || data.length === 0) {
                console.warn('No injury data received or data is not an array');
                return;
            }

            const fragment = document.createDocumentFragment();
            for (let injury of data) {
                console.log('Processing injury:', injury);
                const row = document.createElement('tr');

                // Create image cell
                const logoCell = document.createElement('td');
                const logoImg = document.createElement('img');
                logoImg.src = injury[0] || '';
                logoImg.alt = 'Team Logo';
                logoImg.style.width = '30px';
                logoCell.appendChild(logoImg);
                row.appendChild(logoCell);

                // Add other cells
                for (let i = 1; i < 5; i++) {
                    const cell = document.createElement('td');
                    cell.textContent = injury[i] || '';
                    row.appendChild(cell);
                }

                fragment.appendChild(row);
            }
            tableBody.appendChild(fragment);
            console.log('Table population complete');
        })
        .catch(error => console.error('Error fetching or processing injury data:', error));
}



// Call the function when the page loads
window.addEventListener('load', () => {
    setTimeout(() => {
        populateInjuryTable();
        showSlides(1);
    }, 1000);  // 1 second delay
});

window.addEventListener('resize', adjustSlideshow);
