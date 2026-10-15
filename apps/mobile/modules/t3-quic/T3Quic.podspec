Pod::Spec.new do |s|
  s.name = 'T3Quic'
  s.version = '1.0.0'
  s.summary = 'Native WebTransport client for T3 Code mobile.'
  s.homepage = 'https://t3tools.com'
  s.license = { :type => 'UNLICENSED' }
  s.author = { 'T3 Tools' => 'hello@t3tools.com' }
  s.source = { :path => '.' }
  s.platforms = { :ios => '18.0' }
  s.swift_version = '6.0'
  s.static_framework = true
  s.dependency 'ExpoModulesCore'

  s.source_files = 'ios/**/*.{h,swift}'
  s.public_header_files = 'ios/T3Quic.h'
  s.preserve_paths = 'rust/**/*', 'scripts/prepare-native.mjs'
  s.libraries = 't3_quic'
  s.pod_target_xcconfig = {
    'DEFINES_MODULE' => 'YES',
    'LIBRARY_SEARCH_PATHS' => '$(inherited) "$(PODS_TARGET_SRCROOT)/build/ios/$(PLATFORM_NAME)"',
    'ENABLE_USER_SCRIPT_SANDBOXING' => 'NO',
  }
  s.user_target_xcconfig = {
    'LIBRARY_SEARCH_PATHS' => "$(inherited) \"#{__dir__}/build/ios/$(PLATFORM_NAME)\"",
  }
  s.script_phase = {
    :name => 'Build T3 QUIC',
    :execution_position => :before_compile,
    :always_out_of_date => '1',
    :script => <<~SH,
      set -eu
      if [ -f "${PODS_ROOT}/../.xcode.env" ]; then . "${PODS_ROOT}/../.xcode.env"; fi
      if [ -f "${PODS_ROOT}/../.xcode.env.local" ]; then . "${PODS_ROOT}/../.xcode.env.local"; fi
      "${NODE_BINARY:-node}" "${PODS_TARGET_SRCROOT}/scripts/prepare-native.mjs" ios
    SH
  }
end
