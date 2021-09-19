{
    let menu = document.querySelector(".menu");
    let container = document.querySelector(".container");
    let menuButton = document.querySelector(".menu-button");
    menuButton.addEventListener('click', function (event){
        changeMenuStatus();
    });
    container.addEventListener('click', function (event) {
        changeMenuStatus();
    });

    function changeMenuStatus() {
        if (menu.classList.contains("fold")){
            menu.classList.remove("fold");
            menu.classList.add("unfold");
            container.classList.remove("full");
            container.classList.add("aside");
        }
        else {
            menu.classList.remove("unfold");
            menu.classList.add("fold");
            container.classList.remove("aside");
            container.classList.add("full");
        }
    }

}