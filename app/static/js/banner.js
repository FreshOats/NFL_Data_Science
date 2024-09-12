const bannerContainer = document.querySelector('.banner-container');
const imageUrls = [
    'static/images/Post_Injuries_Days_Played.png',
    'static/images/Post_Injuries_Duration.png',
    'static/images/Post_Injuries_Duration_Field_Type.png',
    'static/images/Post_Injuries_Field_Type.png',
    'static/images/Post_Injuries_Total.png',
    'static/images/Post_Injuries_Position.png',
    'static/images/Post_Injury_Duration_Position.png',
    'static/images/Post_Concussion_Field_Type.png',
    'static/images/Post_Concussion_Position.png'
];

function loadImages() {
    imageUrls.forEach(url => {
        const img = document.createElement('img');
        img.src = url;
        img.alt = 'Banner Image';
        img.addEventListener('click', () => openModal(url));
        bannerContainer.appendChild(img);
    });
}

// Modal functionality
const modal = document.getElementById('imageModal');
const modalImg = document.getElementById('modalImage');
const captionText = document.getElementById('caption');
const closeBtn = document.getElementsByClassName('close')[0];

function openModal(imgUrl) {
    modal.style.display = "block";
    modalImg.src = imgUrl;
}

closeBtn.onclick = function () {
    modal.style.display = "none";
}

window.onclick = function (event) {
    if (event.target == modal) {
        modal.style.display = "none";
    }
}

document.addEventListener('DOMContentLoaded', function () {
    loadImages();
});


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
