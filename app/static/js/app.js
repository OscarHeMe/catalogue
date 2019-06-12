// Feedback functions
feedback = function (body, title, feedback_class, time_shown) {
    var title = title || 'popMsg';
    var body = body || 'popMshBody';
    var feedback_class = feedback_class || 'success';
    var time_shown = time_shown || 5;

    // Dynamically
    let feedback = document.createElement("DIV");
    feedback.classList.add(`feedback`);
    feedback.classList.add(`feedback-floating`);
    feedback.classList.add(`feedback-${feedback_class}`);
    // Close
    let close = document.createElement("A");
    close.classList.add('close-btn');
    close.innerHTML = "x";
    close.addEventListener('click',function(){
        this.parentNode.parentNode.removeChild(this.parentNode);
    })
    // Title
    let divTitle = document.createElement("DIV");
    divTitle.classList.add('head');
    divTitle.innerHTML = title;
    // Body
    let divBody = document.createElement("DIV");
    divBody.classList.add('body');
    divBody.innerHTML = body;
    feedback.appendChild(close);
    feedback.appendChild(divTitle);
    feedback.appendChild(divBody);
    // Append to document
    document.body.appendChild(feedback);

    const fdbk = `<div class="feedback feedback-floating feedback-${feedback_class}"> \
        <a class="close-btn">x</a>\
        <div class="head">${title}</div>\
        <div class="body">${body}</div>\
    </div>`;

    setTimeout(function () {
        feedback.parentNode.removeChild(feedback);
    }, (time_shown * 1000));

    // Acomodamos el feedback dependindo de la posición del header...
    //if ($('.feedback').length > 0 )
    //    moveFeedback();
}

/*
moveFeedback = function (val) {
    if ($('.feedback').length === 0)
        return false;
    val = val || $('.header-container:first').outerHeight();
    $(document).find('.feedback').css('top', (val + 10) + 'px');
}
*/