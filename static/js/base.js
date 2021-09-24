{
    let menu = document.querySelector(".menu");
    let menuItems = document.querySelectorAll(".menu li");
    let container = document.querySelector(".container");
    let menuButton = document.querySelector(".menu-button");

    function changeMenuStatus() {
        if (menu.classList.contains("fold")){
            menu.classList.remove("fold");
            if (window.innerWidth > 1310) {
                container.classList.remove("full");
                menu.classList.add('unfold');
                container.classList.add('aside');
            } else {
                container.classList.remove("full-fix");
                menu.classList.add('unfold-fix');
                container.classList.add('aside-fix');
            }
        }
        else {
            if (window.innerWidth > 1310) {
                menu.classList.remove('unfold');
                container.classList.remove("aside");
                container.classList.add("full");
            }
            else {
                menu.classList.remove('unfold-fix');
                container.classList.remove("aside-fix");
                container.classList.add("full-fix");
            }
            menu.classList.add("fold");
        }
    }

    function changeContainerHeight() {
        if (container.clientHeight < window.innerHeight) {
            container.style.height = window.innerHeight + 'px';
        }
    }

    function doSizeFix() {
        if (menu.classList.contains('unfold')){
            menu.classList.remove('unfold');
            menu.classList.add("unfold-fix");
            container.classList.remove("aside");
            container.classList.add("aside-fix");
        }
        else {
            container.classList.remove('full');
            container.classList.add('full-fix');
        }
        menuItems.forEach(function (item, key) {
            item.style.width = '150px';
        });
    }

    function undoSizeFix() {
        if (menu.classList.contains('unfold-fix')) {
            menu.classList.remove('unfold-fix');
            menu.classList.add("unfold");
            container.classList.remove("aside-fix");
            container.classList.add("aside");
        }
        else {
            container.classList.remove('full-fix');
            container.classList.add('full');
        }
        menuItems.forEach(function (item, key) {
            item.style.width = '10vw';
        });
    }

    window.addEventListener('load', function (event) {
        changeContainerHeight();
        if (window.innerWidth >= 1310) {
            undoSizeFix();
        }
        else {
            doSizeFix();
        }
    });

    let windowWidth = window.innerWidth;
    window.addEventListener('resize', function (event) {
        changeContainerHeight();
        let newWindowWidth = window.innerWidth;
        if (windowWidth < newWindowWidth && newWindowWidth >= 1310) {
            undoSizeFix();
        }
        else if (windowWidth > newWindowWidth && newWindowWidth < 1310) {
            doSizeFix();
        }
        windowWidth = newWindowWidth;
    });

    menuButton.addEventListener('click', function (event){
        changeMenuStatus();
    });

}