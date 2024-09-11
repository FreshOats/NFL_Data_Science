const bannerContainer = document.querySelector('.banner-container');
const imageUrls = [
    'static/images/Post_Injuries_Days_Played.png'
    , 'static/images/Post_Injuries_Duration.png'
    , 'static/images/Post_Injuries_Duration_Field_Type.png'
    , 'static/images/Post_Injuries_Field_Type.png'
    , 'static/images/Post_Injuries_Total.png'
    , 'static/images/Post_Injuries_Position.png'
    , 'static/images/Post_Injury_Duration_Position.png'
    , 'static/images/Post_Concussion_Field_Type.png'
    , 'static/images/Post_Concussion_Position.png'
];
let currentIndex = 0;


function loadImages() {
    imageUrls.forEach(url => {
        const img = document.createElement('img');
        img.src = url;
        img.alt = 'Banner Image';
        bannerContainer.appendChild(img);
    });
}

loadImages();
