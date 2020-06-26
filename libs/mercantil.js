var regex;
var base_url  = "https://www.genealog.cl/Geneanexus";
function marker(match) {
    return '<span class="yellow lighten-2">' + match + '</span>';
}
function generateId() {
    return Math.random().toString(36).substr(2, 6).toUpperCase();
}
function resaltar(str) {
    return str.replace(new RegExp(regex,'gi'), marker);
}
function resaltarRut(rut, dv, pais, cuit) {
    var rutWithDV = rut + "-" + dv;
    var rutRaw = rut + "-" + dv;
    if (pais.toUpperCase() == 'VENEZUELA') {
        rutWithDV = dv + "-" + rut;
        rutRaw = dv + "-" + rut;
    }
    if (pais.toUpperCase() == 'ARGENTINA') {
        rutWithDV = cuit + "-" + rut + "-" + dv;
        rutRaw = cuit + "-" + rut + "-" + dv;
    }
    if (rutWithDV != '-') {
        if (rutWithDV.match(new RegExp(regex.replace('|', ''),'g'))) {
            return '<span class="yellow">' + rutRaw.replace(/(\d)(?=(\d{3})+&#8209;)/g, '$1.') + '</span>';
        } else {
            return rutRaw.replace(/(\d)(?=(\d{3})+&#8209;)/g, '$1.');
        }
    } else {
        return 'Ver';
    }
}
function generarRutLink(rut, rutRaw, code, nombre_completo, tipo, pais, onlyRut, onlyDv, onlyCuit) {
    var doc = 'rut';
    var rutUrl = '';
    var urlFormato = 1;
    var url = '';
    pais = pais.toUpperCase();
    if (tipo == '2') {
        urlFormato = 2
    }
    if (tipo == '3') {
        urlFormato = 3;
    }
    if (urlFormato == 1) {
        var urlStart = 'persona';
        var pageName = encodeURI(parseURI(nombre_completo));
        var seoWord = '/nombre-y-rut/';
        var rutLink = '#!';
        if (rutRaw.match(/\d/g)) {
            if (pais == 'VENEZUELA') {
                rutRaw = onlyDv + '-' + onlyRut;
                seoWord = '/nombre-y-ci/';
            }
            if (pais == 'ARGENTINA') {
                rutRaw = onlyCuit + '-' + onlyRut + '-' + onlyDv;
                seoWord = '/nombre-y-dni/';
            }
            var rutUrl = encodeURI(rutRaw);
            pageName += '-' + rutUrl;
        } else {
            seoWord = '/nombre/';
        }
        rutLink = base_url + '/' + urlStart + '/' + pais + '/' + code + seoWord + pageName;
    }
    if (urlFormato == 2) {
        var urlStart = 'empresa';
        var pageName = encodeURI(parseURI(nombre_completo));
        var seoWord = '/nombre-y-rut/';
        if (rutRaw.match(/\d/g)) {
            var rutUrl = encodeURI(rutRaw);
            pageName += '-' + rutUrl;
        } else {
            seoWord = '/nombre/';
        }
        rutLink = base_url + '/' + urlStart + '/' + pais + '/' + code + seoWord + pageName;
    }
    if (urlFormato == 3) {
        return base_url + '/lugar/' + pais + '/' + code + '/nombre/' + encodeURI(parseURI(nombre_completo));
    }
    return rutLink;
}

function parseURI(str) {
    str = str.replace(/[ÀÁÂÃÄÅÆàáâãäåæ]/g, 'A');
    str = str.replace(/[ÈÉÊËẼèéêëẽ]/g, 'E');
    str = str.replace(/[ÌÍÎÏĨìíîïĩ]/g, 'I');
    str = str.replace(/[ÒÓÔÕÖØŒðòóôõöøœ]/g, 'O');
    str = str.replace(/[ÙÚÛÜùúûüµ]/g, 'U');
    str = str.replace(/[Çç]/g, 'C');
    str = str.replace(/[Ññ]/g, 'N');
    str = str.replace(/[Ššß]/g, 'S');
    str = str.replace(/[Ÿ¥Ýÿý]/g, 'Y');
    str = str.replace(/[Žž]/g, 'Z');
    str = str.replace(/[^a-zA-Z0-9~.:_\-]/g, '-');
    str = str.toUpperCase();
    return str;
}

function getRutLink(data) {
    var persona = data[Object.keys(data)[0]];
    idRandom = generateId();
    if (typeof persona === 'string') {
        return false;
    }
    var nombre_completo = resaltar(persona['nombre_completo'].toUpperCase());
    var apellido_paterno = '';
    var apellido_materno = '';
    var pais = ('pais' in persona) ? persona['pais'] : 'CHILE';
    var cuit = ('cuit' in persona) ? persona['cuit'] : '';
    pais = pais.toUpperCase();
    rutStr = persona['rut'] + "-" + persona['dv'];
    if (pais == 'VENEZUELA') {
        rutStr = persona['dv'] + "-" + persona['rut'];
    }
    if (pais == 'ARGENTINA') {
        rutStr = cuit + "-" + persona['rut'] + "-" + persona['dv'];
    }
    return generarRutLink(resaltarRut(persona['rut'], persona['dv'], pais, cuit), rutStr, persona['code'], persona['nombre_completo'], persona['tipo_registro'], pais, persona['rut'], persona['dv'], cuit);
}
function setRegex(regexStr){
    regex = regexStr;
}
