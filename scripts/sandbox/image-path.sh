# shellcheck shell=sh
# Resolve a path inside a guest root file system, as a chroot would.
#
# The image controls its symbolic links. An absolute link starts again at
# the image root, and `..` stops there. So no link of the image can take a
# root build step to a host path. A path with more than 40 links fails.
#
# Usage, from sh or bash:
#   . scripts/sandbox/image-path.sh
#   image_path ROOT PATH
image_path() {
  _ip_root="$1"
  _ip_todo="${2#/}"
  _ip_done=""
  _ip_hops=0
  while [ -n "${_ip_todo}" ]; do
    _ip_name="${_ip_todo%%/*}"
    if [ "${_ip_name}" = "${_ip_todo}" ]; then
      _ip_todo=""
    else
      _ip_todo="${_ip_todo#*/}"
    fi
    case "${_ip_name}" in
      "" | .) continue ;;
      ..)
        _ip_done="${_ip_done%/*}"
        continue
        ;;
    esac
    if [ -L "${_ip_root}${_ip_done}/${_ip_name}" ]; then
      _ip_hops=$((_ip_hops + 1))
      if [ "${_ip_hops}" -gt 40 ]; then
        echo "image-path: too many links in $2" >&2
        return 1
      fi
      _ip_target="$(readlink "${_ip_root}${_ip_done}/${_ip_name}")"
      case "${_ip_target}" in
        /*) _ip_done="" ;;
      esac
      _ip_todo="${_ip_target#/}${_ip_todo:+/${_ip_todo}}"
    else
      _ip_done="${_ip_done}/${_ip_name}"
    fi
  done
  printf '%s\n' "${_ip_root}${_ip_done}"
}
